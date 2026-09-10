"""Pydantic request/response models."""
from __future__ import annotations

from pydantic import BaseModel, field_validator

from . import config


class AdjustmentIn(BaseModel):
    product_sk: int
    store_sk: int
    fiscal_year: int
    fiscal_week: int
    ai_forecast_qty: float
    override_qty: float
    override_reason: str
    planner_id: str

    @field_validator("override_reason")
    @classmethod
    def _reason(cls, v: str) -> str:
        if v not in config.REASONS:
            raise ValueError(
                f"override_reason must be one of {config.REASONS}"
            )
        return v

    @field_validator("planner_id")
    @classmethod
    def _planner(cls, v: str) -> str:
        if v not in config.PLANNERS:
            raise ValueError(f"planner_id must be one of {config.PLANNERS}")
        return v

    @field_validator("override_qty")
    @classmethod
    def _qty(cls, v: float) -> float:
        if v < 0:
            raise ValueError("override_qty must be >= 0")
        return v

    @field_validator("fiscal_week")
    @classmethod
    def _week(cls, v: int) -> int:
        if not (1 <= v <= 53):
            raise ValueError("fiscal_week must be between 1 and 53")
        return v
