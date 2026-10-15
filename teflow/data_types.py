"""Shared submission contract for explicitly selected analysis types."""

from __future__ import annotations


DATA_TYPES = ("bulk", "scrna")


def validate_data_type(data_type: str | None) -> str:
    """Return a supported canonical type before any submission side effects."""
    if data_type is None or data_type == "":
        raise ValueError("请选择数据类型（bulk 或 scrna）。")
    if not isinstance(data_type, str):
        raise ValueError("数据类型必须是 bulk 或 scrna。")
    selected = data_type.strip().lower()
    if selected not in DATA_TYPES:
        raise ValueError(f"不支持的数据类型：{data_type}；请选择 bulk 或 scrna。")
    if selected == "scrna":
        raise ValueError("单细胞分析流程待配置，暂不能提交运行。")
    return selected
