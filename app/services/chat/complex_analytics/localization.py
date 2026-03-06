from __future__ import annotations

from typing import Dict

RU_PURPOSE_HINTS = {
    "identifier/key column": "идентификатор/ключ",
    "time/event timestamp": "временная метка события",
    "free-text/narrative field": "текстовое поле (комментарии/описание)",
    "organizational or location dimension": "измерение локации/офиса",
    "process state dimension": "измерение статуса процесса",
    "financial metric field": "финансовая метрика",
    "volume/count metric field": "количественная метрика",
    "attribute used for segmentation/analysis": "атрибут для сегментации/аналитики",
}

RU_PROCESS_CONTEXT = {
    "Application review / processing workflow with distributed offices and analyst comments.": (
        "Процесс обработки заявок с распределением по офисам и комментариями аналитиков."
    ),
    "Order-to-cash / document processing workflow.": "Операционный процесс order-to-cash / обработки документов.",
    "Support or incident management workflow.": "Процесс поддержки или управления инцидентами.",
    "Likely an operational process dataset with records, dimensions, and process indicators.": (
        "Вероятно, это операционный процессный датасет с фактами, измерениями и индикаторами процесса."
    ),
    "Likely an operational process dataset.": "Вероятно, это операционный процессный датасет.",
}

RU_NOTE_MAP = {
    "comment_time exists but could not be parsed to datetime": (
        "Колонка comment_time найдена, но не удалось корректно распознать формат даты/времени."
    ),
    "NLP requested but no tokens extracted from comment_text": (
        "Запрошен NLP-анализ, но из comment_text не удалось извлечь токены."
    ),
    "NLP requested but comment_text column was not found": (
        "Запрошен NLP-анализ, но колонка comment_text не найдена."
    ),
    "Visualization requested but no suitable columns were found for charting.": (
        "Запрошена визуализация, но не найдены подходящие колонки для построения графика."
    ),
}

RU_ARTIFACT_KIND_MAP = {
    "heatmap": "тепловая карта",
    "histogram": "гистограмма",
    "categorical_bar": "категориальная диаграмма",
    "dependency_bar": "диаграмма зависимости",
    "correlation_heatmap": "корреляционная тепловая карта",
    "scatter": "диаграмма рассеяния",
    "chart": "график",
}


def localize_en_to_ru(value: str, mapping: Dict[str, str]) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return mapping.get(text, text)

