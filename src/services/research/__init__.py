"""Immutable research data and snapshot primitives."""

from .canonical import (
    CanonicalJSONError,
    canonical_hash,
    canonical_json,
    canonicalize,
)
from .datasets import DatasetStatus
from .availability import (
    AvailabilityAssessment,
    DATASET_DEFINITIONS,
    DEFAULT_RESEARCH_DATASETS,
    DatasetDefinition,
    assess_dataset_rows,
    to_tushare_ts_code,
)
from .raw_store import RawArtifactReference, RawArtifactStore
from .repositories import (
    DatasetSnapshotInput,
    FactorSnapshotInput,
    LeaseFence,
    ResearchSnapshotInput,
    ResearchSnapshotRepository,
    SnapshotWriteResult,
)
from .collector import (
    DatasetCollectionResult,
    ResearchCollectionResult,
    ResearchDatasetCollector,
)
from .snapshot_service import (
    FACTOR_ENGINE_VERSION,
    FIELD_DICTIONARY_VERSION,
    SNAPSHOT_VERSION,
    FrozenResearchSnapshot,
    build_research_snapshot,
    model_route_fingerprint,
    persist_research_snapshot,
    project_factors,
    project_model_route,
    project_structured_datasets,
    safe_project_context_pack,
)

__all__ = [
    "CanonicalJSONError",
    "AvailabilityAssessment",
    "DATASET_DEFINITIONS",
    "DEFAULT_RESEARCH_DATASETS",
    "DatasetCollectionResult",
    "DatasetDefinition",
    "DatasetSnapshotInput",
    "DatasetStatus",
    "FactorSnapshotInput",
    "FACTOR_ENGINE_VERSION",
    "FIELD_DICTIONARY_VERSION",
    "FrozenResearchSnapshot",
    "LeaseFence",
    "RawArtifactReference",
    "RawArtifactStore",
    "ResearchSnapshotInput",
    "ResearchCollectionResult",
    "ResearchDatasetCollector",
    "ResearchSnapshotRepository",
    "SnapshotWriteResult",
    "SNAPSHOT_VERSION",
    "build_research_snapshot",
    "assess_dataset_rows",
    "canonical_hash",
    "canonical_json",
    "canonicalize",
    "model_route_fingerprint",
    "persist_research_snapshot",
    "project_factors",
    "project_model_route",
    "project_structured_datasets",
    "safe_project_context_pack",
    "to_tushare_ts_code",
]
