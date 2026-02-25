"""Bayesian evidence aggregation types.

Plain dataclasses for the forecasting engine.  No external deps.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Evidence:
    """A single piece of evidence feeding the Bayesian aggregation."""

    id: str
    claim: str
    polarity: int  # +1 or -1
    type: str  # "A", "B", "C", or "D"
    origin_id: str  # cluster key — same origin_id items get grouped
    verifiability: float  # [0, 1]
    corroborations_indep: int  # independent corroboration count
    consistency: float  # [0, 1]
    first_report: bool = False
    published_at: Optional[datetime] = None
    urls: list[str] = field(default_factory=list)
    log_lr_hint: Optional[float] = None  # pre-computed override


@dataclass
class ClusterMeta:
    """Metadata for one origin cluster after aggregation."""

    cluster_id: str
    size: int
    rho: float
    effective_count: float
    mean_log_lr: float
    contribution: float  # effective_count * mean_log_lr


@dataclass
class InfluenceItem:
    """Leave-one-out influence of a single evidence item."""

    evidence_id: str
    claim: str
    polarity: int
    delta_pp: float  # |p_neutral - p_without_this_evidence|


@dataclass
class ForecastResult:
    """Full output of a Bayesian aggregation run."""

    prior: float  # p0
    p_neutral: float  # posterior from evidence only
    p_blended: float  # after market-price blending
    market_prob: float  # market price used for blending
    edge_percent: float  # (p_blended - market_prob) * 100
    evidence: list[Evidence]
    clusters: list[ClusterMeta]
    influence: list[InfluenceItem]
    evidence_count_for: int  # polarity == +1
    evidence_count_against: int  # polarity == -1
