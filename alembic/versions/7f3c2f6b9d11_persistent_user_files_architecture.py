"""persistent user files architecture

Revision ID: 7f3c2f6b9d11
Revises: a02c4d1bd39d
Create Date: 2026-03-23 16:05:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7f3c2f6b9d11"
down_revision: Union[str, Sequence[str], None] = "a02c4d1bd39d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_table("conversation_files")
    op.drop_table("files")

    op.create_table(
        "files",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("original_filename", sa.String(length=512), nullable=False),
        sa.Column("stored_filename", sa.String(length=512), nullable=False),
        sa.Column("storage_key", sa.String(length=1024), nullable=False),
        sa.Column("storage_path", sa.String(length=2048), nullable=False),
        sa.Column("mime_type", sa.String(length=255), nullable=True),
        sa.Column("extension", sa.String(length=32), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("checksum", sa.String(length=128), nullable=True),
        sa.Column("visibility", sa.String(length=32), nullable=False, server_default="private"),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="uploaded"),
        sa.Column("source_kind", sa.String(length=64), nullable=False, server_default="upload"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("deleted_at", sa.DateTime(), nullable=True),
        sa.Column("chunks_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("embedding_model", sa.String(length=255), nullable=True),
        sa.Column("processed_at", sa.DateTime(), nullable=True),
        sa.Column("custom_metadata", sa.JSON(), nullable=True),
        sa.Column("content_preview", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("storage_key", name="uq_files_storage_key"),
    )
    op.create_index("ix_files_user_id", "files", ["user_id"], unique=False)
    op.create_index("ix_files_status", "files", ["status"], unique=False)

    op.create_table(
        "chat_file_links",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("chat_id", sa.UUID(), nullable=False),
        sa.Column("file_id", sa.UUID(), nullable=False),
        sa.Column("attached_by_user_id", sa.UUID(), nullable=False),
        sa.Column("attached_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["chat_id"], ["conversations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["file_id"], ["files.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["attached_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("chat_id", "file_id", name="uq_chat_file_links_chat_file"),
    )
    op.create_index("ix_chat_file_links_chat_id", "chat_file_links", ["chat_id"], unique=False)
    op.create_index("ix_chat_file_links_file_id", "chat_file_links", ["file_id"], unique=False)

    op.create_table(
        "file_processing_profiles",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("file_id", sa.UUID(), nullable=False),
        sa.Column("pipeline_version", sa.String(length=64), nullable=False),
        sa.Column("parser_version", sa.String(length=64), nullable=False),
        sa.Column("artifact_version", sa.String(length=64), nullable=False),
        sa.Column("embedding_provider", sa.String(length=64), nullable=False),
        sa.Column("embedding_model", sa.String(length=255), nullable=True),
        sa.Column("embedding_dimension", sa.Integer(), nullable=True),
        sa.Column("chunking_strategy", sa.String(length=128), nullable=True),
        sa.Column("retrieval_profile", sa.String(length=128), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="queued"),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("ingestion_progress", sa.JSON(), nullable=True),
        sa.Column("artifact_metadata", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["file_id"], ["files.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_file_processing_profiles_file_id", "file_processing_profiles", ["file_id"], unique=False)
    op.create_index("ix_file_processing_profiles_status", "file_processing_profiles", ["status"], unique=False)
    op.create_index(
        "ix_file_processing_profiles_file_id_is_active",
        "file_processing_profiles",
        ["file_id", "is_active"],
        unique=False,
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_file_processing_profiles_active ON file_processing_profiles (file_id) WHERE is_active = true"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_file_processing_profiles_active")
    op.drop_index("ix_file_processing_profiles_file_id_is_active", table_name="file_processing_profiles")
    op.drop_index("ix_file_processing_profiles_status", table_name="file_processing_profiles")
    op.drop_index("ix_file_processing_profiles_file_id", table_name="file_processing_profiles")
    op.drop_table("file_processing_profiles")

    op.drop_index("ix_chat_file_links_file_id", table_name="chat_file_links")
    op.drop_index("ix_chat_file_links_chat_id", table_name="chat_file_links")
    op.drop_table("chat_file_links")

    op.drop_index("ix_files_status", table_name="files")
    op.drop_index("ix_files_user_id", table_name="files")
    op.drop_table("files")

    op.create_table(
        "files",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("filename", sa.String(length=255), nullable=False),
        sa.Column("original_filename", sa.String(length=255), nullable=False),
        sa.Column("path", sa.String(length=500), nullable=False),
        sa.Column("file_type", sa.String(length=50), nullable=True),
        sa.Column("file_size", sa.Integer(), nullable=True),
        sa.Column("content_preview", sa.Text(), nullable=True),
        sa.Column("is_processed", sa.String(length=20), nullable=True),
        sa.Column("chunks_count", sa.Integer(), nullable=True),
        sa.Column("embedding_model", sa.String(length=100), nullable=True),
        sa.Column("processed_at", sa.DateTime(), nullable=True),
        sa.Column("custom_metadata", sa.JSON(), nullable=True),
        sa.Column("uploaded_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "conversation_files",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("conversation_id", sa.UUID(), nullable=False),
        sa.Column("file_id", sa.UUID(), nullable=False),
        sa.Column("added_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["conversation_id"], ["conversations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["file_id"], ["files.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
