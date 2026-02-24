# app/api/v1/endpoints/chat.py
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List, Tuple, Dict, Any
import logging
import json
import uuid
from datetime import datetime

from app.db.session import get_db
from app.db.models import User
from app.schemas import ChatMessage, ChatResponse
from app.api.dependencies import get_current_user_optional
from app.services.llm.manager import llm_manager
from app.crud import crud_conversation, crud_message, crud_file
from app.rag.retriever import rag_retriever

router = APIRouter()
logger = logging.getLogger(__name__)


def _build_conversation_history(messages):
    return [{"role": msg.role, "content": msg.content} for msg in messages[:-1]]


def _normalize_source(source: Optional[str]) -> str:
    src = (source or "").strip().lower()
    if src == "corporate":
        return "aihub"
    if src in ("aihub", "openai", "ollama", "local"):
        return src
    return "local"


def _parse_file_embedding_meta(raw_value: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    raw = (raw_value or "").strip()
    if not raw:
        return None, None

    if ":" in raw:
        mode_raw, model_raw = raw.split(":", 1)
        mode = _normalize_source(mode_raw)
        model = model_raw.strip() or None
        if mode in ("local", "ollama", "aihub"):
            return ("local" if mode == "ollama" else mode), model

    # legacy format where only model was stored
    return None, raw


def _resolve_rag_embedding_config(files, requested_model_source: Optional[str]) -> Tuple[str, Optional[str]]:
    fallback_mode = "aihub" if _normalize_source(requested_model_source) == "aihub" else "local"

    first_model_only: Optional[str] = None
    for f in files:
        mode, model = _parse_file_embedding_meta(getattr(f, "embedding_model", None))
        if model and not first_model_only:
            first_model_only = model
        if mode:
            return mode, model

    return fallback_mode, first_model_only


def _build_rag_conversation_memory(history: List[Dict[str, str]], max_messages: int = 6) -> List[Dict[str, str]]:
    if not history:
        return []
    tail = history[-max_messages:]
    return [{"role": m.get("role", "user"), "content": (m.get("content") or "")[:1500]} for m in tail]


def _build_critic_context(context_documents: List[Dict[str, Any]], max_chars: int = 12000) -> str:
    parts: List[str] = []
    used = 0
    for i, d in enumerate(context_documents, start=1):
        meta = d.get("metadata") or {}
        filename = meta.get("filename") or "unknown"
        chunk_index = meta.get("chunk_index", "?")
        content = (d.get("content") or "").strip()
        if not content:
            continue
        block = f"[{i}] file={filename} chunk={chunk_index}\n{content}\n"
        if used + len(block) > max_chars:
            remain = max_chars - used
            if remain <= 0:
                break
            block = block[:remain]
        parts.append(block)
        used += len(block)
        if used >= max_chars:
            break
    return "\n---\n".join(parts)


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    raw = (text or "").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None

    candidate = raw[start : end + 1]
    try:
        return json.loads(candidate)
    except Exception:
        return None


async def _run_answer_critic(
    *,
    query: str,
    answer: str,
    context_documents: List[Dict[str, Any]],
    model_source: Optional[str],
    model_name: Optional[str],
) -> Tuple[str, Dict[str, Any]]:
    context_text = _build_critic_context(context_documents, max_chars=12000)
    if not context_text:
        return answer, {"enabled": True, "applied": False, "reason": "empty_context"}

    critic_prompt = (
        "You are an answer quality critic for RAG.\n"
        "Given user question, draft answer, and evidence context, evaluate factual support.\n"
        "Return STRICT JSON object with fields:\n"
        "supported: boolean,\n"
        "issues: array of short strings,\n"
        "missing_points: array of short strings,\n"
        "refined_answer: string,\n"
        "confidence: number (0..1).\n"
        "Do not return markdown.\n\n"
        f"Question:\n{query}\n\n"
        f"Draft answer:\n{answer}\n\n"
        f"Evidence context:\n{context_text}\n\n"
        "JSON:"
    )

    try:
        critic = await llm_manager.generate_response(
            prompt=critic_prompt,
            model_source=model_source,
            model_name=model_name,
            temperature=0.0,
            max_tokens=1200,
            conversation_history=None,
        )
    except Exception as e:
        logger.warning("Critic step failed: %s", e)
        return answer, {"enabled": True, "applied": False, "reason": "critic_call_failed"}

    parsed = _extract_json_object(critic.get("response", ""))
    if not parsed:
        return answer, {"enabled": True, "applied": False, "reason": "critic_parse_failed"}

    supported = bool(parsed.get("supported", True))
    refined = (parsed.get("refined_answer") or "").strip()
    confidence = parsed.get("confidence")
    issues = parsed.get("issues") if isinstance(parsed.get("issues"), list) else []
    missing = parsed.get("missing_points") if isinstance(parsed.get("missing_points"), list) else []

    apply_refine = (not supported and bool(refined)) or (bool(refined) and refined != answer and len(refined) > 20)
    final = refined if apply_refine else answer

    return final, {
        "enabled": True,
        "applied": bool(apply_refine),
        "supported": supported,
        "confidence": confidence,
        "issues_count": len(issues),
        "missing_points_count": len(missing),
    }


def _batch_context_docs(context_documents: List[Dict[str, Any]], max_docs: int = 12, max_chars: int = 7000) -> List[List[Dict[str, Any]]]:
    batches: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    chars = 0

    for d in context_documents:
        content = (d.get("content") or "").strip()
        if not content:
            continue
        add = len(content)

        if current and (len(current) >= max_docs or (chars + add) > max_chars):
            batches.append(current)
            current = []
            chars = 0

        current.append(d)
        chars += add

    if current:
        batches.append(current)

    return batches


async def _build_full_file_map_reduce_prompt(
    *,
    query: str,
    context_documents: List[Dict[str, Any]],
    model_source: Optional[str],
    model_name: Optional[str],
) -> str:
    batches = _batch_context_docs(context_documents, max_docs=12, max_chars=7000)
    if not batches:
        return query

    partials: List[str] = []
    max_batches = 20

    for i, batch in enumerate(batches[:max_batches], start=1):
        chunk_lines: List[str] = []
        for j, d in enumerate(batch, start=1):
            meta = d.get("metadata") or {}
            filename = meta.get("filename") or "unknown"
            chunk_index = meta.get("chunk_index", "?")
            content = (d.get("content") or "").strip()
            if not content:
                continue
            chunk_lines.append(f"[{j}] file={filename} chunk={chunk_index}\n{content}")

        if not chunk_lines:
            continue

        map_prompt = (
            "You are summarizing one batch of a large document.\n"
            "Extract key facts relevant to the user question.\n"
            "If the batch has no relevant facts, explicitly say so.\n"
            "Be concise and factual.\n\n"
            f"User question:\n{query}\n\n"
            f"Batch content ({i}/{min(len(batches), max_batches)}):\n"
            + "\n\n---\n\n".join(chunk_lines)
            + "\n\nBatch summary:"
        )

        try:
            map_result = await llm_manager.generate_response(
                prompt=map_prompt,
                model_source=model_source,
                model_name=model_name,
                temperature=0.1,
                max_tokens=900,
                conversation_history=None,
            )
            partial_text = (map_result.get("response") or "").strip()
            if partial_text:
                partials.append(f"[PART {i}]\n{partial_text}")
        except Exception:
            logger.warning("Map step failed for batch %d", i, exc_info=True)

    if not partials:
        return query

    reduce_context = "\n\n=====\n\n".join(partials)
    final_prompt = (
        "You are a document analyst. You received summaries of all document parts.\n"
        "Produce a complete, consistent answer to the user question.\n"
        "Rules:\n"
        "1) Do not invent facts outside provided context.\n"
        "2) Explicitly mention missing information when needed.\n"
        "3) Provide a structured answer with key findings.\n"
        "4) Use all available summaries, not a single fragment.\n\n"
        f"User question:\n{query}\n\n"
        f"All partial summaries:\n{reduce_context}\n\n"
        "Final answer:"
    )
    return final_prompt


async def _try_build_rag_prompt(
    *,
    db: AsyncSession,
    user_id: Optional[uuid.UUID],
    conversation_id: uuid.UUID,
    query: str,
    top_k: int = 3,
    file_ids: Optional[List[str]] = None,
    model_source: Optional[str] = None,
    model_name: Optional[str] = None,
):
    final_prompt = query
    rag_used = False
    rag_debug = None
    context_docs: List[Dict[str, Any]] = []

    if not user_id:
        return final_prompt, rag_used, rag_debug, context_docs

    try:
        files = await crud_file.get_conversation_files(db, conversation_id=conversation_id, user_id=user_id)
        logger.info("Conversation files (completed): %d", len(files))
    except Exception as e:
        logger.warning("Could not fetch conversation files: %s", e)
        return final_prompt, rag_used, rag_debug, context_docs

    if file_ids:
        allowed_ids = {str(x) for x in file_ids}
        files = [f for f in files if str(f.id) in allowed_ids]
        logger.info("Conversation files filtered by payload file_ids: %d", len(files))

    if not files:
        return final_prompt, rag_used, rag_debug, context_docs

    rag_file_ids = [str(f.id) for f in files]
    embedding_mode, embedding_model = _resolve_rag_embedding_config(files, model_source)

    try:
        rag_result = await rag_retriever.query_rag(
            query,
            top_k=top_k,
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            file_ids=rag_file_ids,
            embedding_mode=embedding_mode,
            embedding_model=embedding_model,
            debug_return=True,
        )

        if isinstance(rag_result, dict) and "docs" in rag_result:
            context_docs = rag_result.get("docs") or []
            rag_debug = rag_result.get("debug")
        else:
            context_docs = rag_result or []

        if isinstance(rag_debug, dict):
            rag_debug["embedding_mode"] = embedding_mode
            rag_debug["embedding_model"] = embedding_model
            rag_debug["file_ids"] = rag_file_ids

        if context_docs:
            retrieval_mode = (rag_debug or {}).get("retrieval_mode") if isinstance(rag_debug, dict) else None
            intent = (rag_debug or {}).get("intent") if isinstance(rag_debug, dict) else None

            if retrieval_mode == "full_file" or intent == "analyze_full_file":
                final_prompt = await _build_full_file_map_reduce_prompt(
                    query=query,
                    context_documents=context_docs,
                    model_source=model_source,
                    model_name=model_name,
                )
            else:
                final_prompt = rag_retriever.build_context_prompt(query=query, context_documents=context_docs)

            rag_used = True
            logger.info(
                "RAG enabled: docs=%d mode=%s model=%s retrieval_mode=%s",
                len(context_docs),
                embedding_mode,
                embedding_model,
                retrieval_mode,
            )
        else:
            logger.info("RAG: no relevant chunks")

    except TypeError:
        # Compatibility fallback for older query_rag signatures
        context_docs = await rag_retriever.query_rag(
            query,
            top_k=top_k,
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            debug_return=True,
        )
        if isinstance(context_docs, dict) and "docs" in context_docs:
            context_docs_list = context_docs.get("docs") or []
            rag_debug = context_docs.get("debug")
            if context_docs_list:
                final_prompt = rag_retriever.build_context_prompt(query=query, context_documents=context_docs_list)
                rag_used = True

    except Exception as e:
        logger.warning("RAG retrieval failed: %s", e)

    return final_prompt, rag_used, rag_debug, context_docs


@router.post("/stream")
async def chat_stream(
    chat_data: ChatMessage,
    db: AsyncSession = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional),
):
    try:
        user_id = current_user.id if current_user else None
        username = current_user.username if current_user else "anonymous"

        logger.info("Chat(stream) from %s", username)

        # Get or create conversation
        if chat_data.conversation_id:
            conversation = await crud_conversation.get(db, id=chat_data.conversation_id)
            if not conversation:
                raise HTTPException(status_code=404, detail="Conversation not found")
            if conversation.user_id != user_id:
                raise HTTPException(status_code=403, detail="Access denied")
            conversation_id = conversation.id
        else:
            from app.schemas.conversation import ConversationCreate

            conv_data = ConversationCreate(
                title=chat_data.message[:100] if len(chat_data.message) <= 100 else chat_data.message[:97] + "...",
                model_source=chat_data.model_source or "ollama",
                model_name=chat_data.model_name or llm_manager.ollama_model,
            )
            conversation = await crud_conversation.create_for_user(db=db, obj_in=conv_data, user_id=user_id)
            conversation_id = conversation.id

        # Save user message
        await crud_message.create_message(db=db, conversation_id=conversation_id, role="user", content=chat_data.message)

        # History
        messages = await crud_message.get_conversation_messages(db, conversation_id=conversation_id)
        conversation_history = _build_conversation_history(messages)

        # RAG
        final_prompt, rag_used, rag_debug, context_docs = await _try_build_rag_prompt(
            db=db,
            user_id=user_id,
            conversation_id=conversation_id,
            query=chat_data.message,
            top_k=8,
            file_ids=chat_data.file_ids,
            model_source=chat_data.model_source,
            model_name=chat_data.model_name,
        )

        assistant_message_id = uuid.uuid4()

        async def event_stream():
            full_response = ""
            start_time = datetime.utcnow()

            try:
                yield f"data: {json.dumps({'type': 'start','conversation_id': str(conversation_id),'message_id': str(assistant_message_id),'rag_enabled': rag_used,'rag_debug': rag_debug})}\n\n"

                history_for_generation = conversation_history
                if rag_used:
                    history_for_generation = _build_rag_conversation_memory(conversation_history, max_messages=6)

                async for chunk in llm_manager.generate_response_stream(
                    prompt=final_prompt,
                    model_source=chat_data.model_source,
                    model_name=chat_data.model_name,
                    temperature=chat_data.temperature or 0.7,
                    max_tokens=chat_data.max_tokens or 2000,
                    conversation_history=history_for_generation,
                ):
                    full_response += chunk
                    yield f"data: {json.dumps({'type': 'chunk', 'content': chunk})}\n\n"

                generation_time = (datetime.utcnow() - start_time).total_seconds()

                if rag_used and context_docs:
                    refined_response, critic_meta = await _run_answer_critic(
                        query=chat_data.message,
                        answer=full_response,
                        context_documents=context_docs,
                        model_source=chat_data.model_source,
                        model_name=chat_data.model_name,
                    )
                    if refined_response != full_response:
                        full_response = refined_response
                        yield f"data: {json.dumps({'type': 'final_refinement', 'content': refined_response, 'critic': critic_meta})}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': 'critic', 'critic': critic_meta})}\n\n"

                await crud_message.create_message(
                    db=db,
                    conversation_id=conversation_id,
                    role="assistant",
                    content=full_response,
                    model_name=chat_data.model_name or llm_manager.ollama_model,
                    temperature=chat_data.temperature,
                    max_tokens=chat_data.max_tokens,
                    generation_time=generation_time,
                )

                yield f"data: {json.dumps({'type': 'done', 'generation_time': generation_time, 'rag_used': rag_used, 'critic_applied': bool(rag_used and context_docs)})}\n\n"

            except Exception as e:
                logger.error("Streaming error: %s", e, exc_info=True)
                yield f"data: {json.dumps({'type':'error','message': str(e), 'error_type': type(e).__name__})}\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Chat stream error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Chat failed: {str(e)}")


@router.post("/", response_model=ChatResponse)
async def chat(
    chat_data: ChatMessage,
    db: AsyncSession = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional),
):
    try:
        user_id = current_user.id if current_user else None
        username = current_user.username if current_user else "anonymous"
        logger.info("Chat(non-stream) from %s", username)

        if chat_data.conversation_id:
            conversation = await crud_conversation.get(db, id=chat_data.conversation_id)
            if not conversation:
                raise HTTPException(status_code=404, detail="Conversation not found")
            if conversation.user_id != user_id:
                raise HTTPException(status_code=403, detail="Access denied")
            conversation_id = conversation.id
        else:
            from app.schemas.conversation import ConversationCreate

            conv_data = ConversationCreate(
                title=chat_data.message[:100] if len(chat_data.message) <= 100 else chat_data.message[:97] + "...",
                model_source=chat_data.model_source or "ollama",
                model_name=chat_data.model_name or llm_manager.ollama_model,
            )
            conversation = await crud_conversation.create_for_user(db=db, obj_in=conv_data, user_id=user_id)
            conversation_id = conversation.id

        await crud_message.create_message(db=db, conversation_id=conversation_id, role="user", content=chat_data.message)

        messages = await crud_message.get_conversation_messages(db, conversation_id=conversation_id)
        conversation_history = _build_conversation_history(messages)

        final_prompt, rag_used, rag_debug, context_docs = await _try_build_rag_prompt(
            db=db,
            user_id=user_id,
            conversation_id=conversation_id,
            query=chat_data.message,
            top_k=8,
            file_ids=chat_data.file_ids,
            model_source=chat_data.model_source,
            model_name=chat_data.model_name,
        )

        history_for_generation = conversation_history
        if rag_used:
            history_for_generation = _build_rag_conversation_memory(conversation_history, max_messages=6)

        start_time = datetime.utcnow()
        result = await llm_manager.generate_response(
            prompt=final_prompt,
            model_source=chat_data.model_source,
            model_name=chat_data.model_name,
            temperature=chat_data.temperature or 0.7,
            max_tokens=chat_data.max_tokens or 2000,
            conversation_history=history_for_generation,
        )

        critic_meta: Optional[Dict[str, Any]] = None
        if rag_used and context_docs:
            refined_answer, critic_meta = await _run_answer_critic(
                query=chat_data.message,
                answer=result.get("response", ""),
                context_documents=context_docs,
                model_source=chat_data.model_source,
                model_name=chat_data.model_name,
            )
            result["response"] = refined_answer
            logger.info("RAG critic(non-stream): %s", critic_meta)

        generation_time = (datetime.utcnow() - start_time).total_seconds()

        assistant_message = await crud_message.create_message(
            db=db,
            conversation_id=conversation_id,
            role="assistant",
            content=result["response"],
            model_name=result["model"],
            temperature=chat_data.temperature,
            max_tokens=chat_data.max_tokens,
            tokens_used=result.get("tokens_used"),
            generation_time=generation_time,
        )

        return ChatResponse(
            response=result["response"],
            conversation_id=conversation_id,
            message_id=assistant_message.id,
            model_used=result["model"],
            tokens_used=result.get("tokens_used"),
            generation_time=generation_time,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Chat error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Chat failed: {str(e)}")
