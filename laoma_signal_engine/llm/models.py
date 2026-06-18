"""Pydantic models for LLM assist output (STEP6.0). docs/STEP6.0_LLM_DeepSeek_TwoFactorSnapshots_任务卡.md."""

from __future__ import annotations

from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

LlmBias = Literal["LONG", "SHORT", "NEUTRAL", "UNSURE"]


class LlmAssistDecisionItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    symbol: str
    llm_bias: LlmBias
    action_hint: str = ""
    confidence_0_100: int = Field(default=50, ge=0, le=100)
    key_reasons: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    @field_validator("key_reasons", "warnings", mode="before")
    @classmethod
    def _coerce_str_lists(cls, v: object) -> object:
        if v is None:
            return []
        return v


class LlmAssistDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    generated_at: str
    source: Literal["llm_deepseek_assist"] = "llm_deepseek_assist"
    input_factor_path: str
    input_factor_generated_at: str
    input_factor_source: str
    prompt_file: str
    model: str
    status: Literal["ok", "error"] = "ok"
    error_message: str = ""
    count: int = 0
    decisions: list[LlmAssistDecisionItem] = Field(default_factory=list)
    raw_usage: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _count_matches(self) -> Self:
        if self.count != len(self.decisions):
            raise ValueError(f"count must equal len(decisions); got {self.count} vs {len(self.decisions)}")
        return self
