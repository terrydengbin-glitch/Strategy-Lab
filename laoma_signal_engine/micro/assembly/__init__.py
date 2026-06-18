"""STEP3.7 Feature assembly package."""

from __future__ import annotations

from laoma_signal_engine.micro.assembly.assembler import (
    AssemblyTargetRow,
    build_document,
    build_micro_15m_block,
    snapshot_to_micro_quality,
)
from laoma_signal_engine.micro.assembly.models import (
    CoverageSummaryBlock,
    DroppedEventsBlock,
    LatestMicroFeaturesDocument,
    LatestMicroStatus,
    Micro15mBlock,
    MicroFeatureItem,
    MicroQualityBlock,
    MicroSignalBlock,
    TargetStatus,
)
from laoma_signal_engine.micro.assembly.writer import atomic_write_json

__all__ = [
    "AssemblyTargetRow",
    "CoverageSummaryBlock",
    "DroppedEventsBlock",
    "LatestMicroFeaturesDocument",
    "LatestMicroStatus",
    "Micro15mBlock",
    "MicroFeatureItem",
    "MicroQualityBlock",
    "MicroSignalBlock",
    "TargetStatus",
    "atomic_write_json",
    "build_document",
    "build_micro_15m_block",
    "snapshot_to_micro_quality",
]
