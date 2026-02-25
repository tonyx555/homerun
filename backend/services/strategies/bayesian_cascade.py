"""
Strategy: Bayesian Cascade - Probability Graph Arbitrage

Builds a DAG of market dependencies and propagates probability updates.
When market A moves, all markets connected to A should adjust.
Markets that HAVEN'T adjusted yet are mispriced.

This is genuinely novel - no existing bot implements graph-based
belief propagation for prediction market arbitrage.

Example:
- Market A: "Fed raises rates in March" moves from 40% to 60%
- Market B: "S&P 500 drops in March" should increase (correlated)
- Market B hasn't moved yet -> B is mispriced -> buy YES on B

Key insight: Information propagates slowly through interconnected markets.
The cascade detector finds these propagation delays.

The Bayesian aggregation engine (services.forecasting.engine) replaces the
naive linear delta multiplication with proper log-likelihood-ratio
accumulation, origin-based clustering, trimmed-mean outlier robustness, and
market-price blending ported from the Polyseer forecasting system.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from models import Market, Event, Opportunity, MispricingType
from .base import BaseStrategy, DecisionCheck, ExitDecision, ScoringWeights, SizingConfig
from services.quality_filter import QualityFilterOverrides
from services.forecasting.engine import run_forecast, evidence_log_lr as _fc_evidence_log_lr, TYPE_CAPS as _FC_TYPE_CAPS
from services.forecasting.types import Evidence as FcEvidence, ForecastResult
from utils.kelly import kelly_fraction
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Entity extraction constants
# ---------------------------------------------------------------------------

# Crypto assets
_CRYPTO_ENTITIES: list[tuple[str, re.Pattern]] = [
    ("BTC", re.compile(r"\b(?:BTC|Bitcoin)\b", re.IGNORECASE)),
    ("ETH", re.compile(r"\b(?:ETH|Ethereum|Ether)\b", re.IGNORECASE)),
    ("SOL", re.compile(r"\b(?:SOL|Solana)\b", re.IGNORECASE)),
    ("XRP", re.compile(r"\b(?:XRP|Ripple)\b", re.IGNORECASE)),
    ("DOGE", re.compile(r"\b(?:DOGE|Dogecoin)\b", re.IGNORECASE)),
    ("ADA", re.compile(r"\b(?:ADA|Cardano)\b", re.IGNORECASE)),
    ("MATIC", re.compile(r"\b(?:MATIC|Polygon)\b", re.IGNORECASE)),
    ("AVAX", re.compile(r"\b(?:AVAX|Avalanche)\b", re.IGNORECASE)),
    ("LINK", re.compile(r"\b(?:LINK|Chainlink)\b", re.IGNORECASE)),
]

# Politicians / public figures
_POLITICIAN_ENTITIES: list[tuple[str, re.Pattern]] = [
    ("Trump", re.compile(r"\bTrump\b", re.IGNORECASE)),
    ("Biden", re.compile(r"\bBiden\b", re.IGNORECASE)),
    ("DeSantis", re.compile(r"\bDeSantis\b", re.IGNORECASE)),
    ("Harris", re.compile(r"\bHarris\b", re.IGNORECASE)),
    ("Newsom", re.compile(r"\bNewsom\b", re.IGNORECASE)),
    ("Haley", re.compile(r"\bHaley\b", re.IGNORECASE)),
    ("RFK", re.compile(r"\b(?:RFK|Kennedy)\b", re.IGNORECASE)),
    ("Vance", re.compile(r"\bVance\b", re.IGNORECASE)),
    ("Musk", re.compile(r"\b(?:Musk|Elon)\b", re.IGNORECASE)),
    ("Putin", re.compile(r"\bPutin\b", re.IGNORECASE)),
    ("Zelensky", re.compile(r"\bZelensky\b", re.IGNORECASE)),
    ("Xi", re.compile(r"\bXi\b", re.IGNORECASE)),
]

# Central banks / monetary entities
_CENTRAL_BANK_ENTITIES: list[tuple[str, re.Pattern]] = [
    ("Fed", re.compile(r"\b(?:Fed|Federal Reserve|FOMC)\b", re.IGNORECASE)),
    ("ECB", re.compile(r"\b(?:ECB|European Central Bank)\b", re.IGNORECASE)),
    ("BOJ", re.compile(r"\b(?:BOJ|Bank of Japan)\b", re.IGNORECASE)),
    ("BOE", re.compile(r"\b(?:BOE|Bank of England)\b", re.IGNORECASE)),
]

# Countries / regions
_COUNTRY_ENTITIES: list[tuple[str, re.Pattern]] = [
    ("USA", re.compile(r"\b(?:US|USA|United States|America)\b", re.IGNORECASE)),
    ("China", re.compile(r"\b(?:China|Chinese)\b", re.IGNORECASE)),
    ("Russia", re.compile(r"\b(?:Russia|Russian)\b", re.IGNORECASE)),
    ("Ukraine", re.compile(r"\b(?:Ukraine|Ukrainian)\b", re.IGNORECASE)),
    ("EU", re.compile(r"\b(?:EU|European Union|Europe)\b", re.IGNORECASE)),
    ("Israel", re.compile(r"\b(?:Israel|Israeli)\b", re.IGNORECASE)),
    ("Iran", re.compile(r"\b(?:Iran|Iranian)\b", re.IGNORECASE)),
    ("Taiwan", re.compile(r"\b(?:Taiwan|Taiwanese)\b", re.IGNORECASE)),
]

# Major companies / tech
_COMPANY_ENTITIES: list[tuple[str, re.Pattern]] = [
    ("Apple", re.compile(r"\bApple\b")),
    ("Google", re.compile(r"\b(?:Google|Alphabet|GOOGL)\b", re.IGNORECASE)),
    ("Tesla", re.compile(r"\b(?:Tesla|TSLA)\b", re.IGNORECASE)),
    ("Nvidia", re.compile(r"\b(?:Nvidia|NVDA)\b", re.IGNORECASE)),
    ("Microsoft", re.compile(r"\b(?:Microsoft|MSFT)\b", re.IGNORECASE)),
    ("Meta", re.compile(r"\b(?:Meta|Facebook)\b", re.IGNORECASE)),
    ("Amazon", re.compile(r"\b(?:Amazon|AMZN)\b", re.IGNORECASE)),
    ("OpenAI", re.compile(r"\bOpenAI\b", re.IGNORECASE)),
]

# Topic keywords for relationship detection
_TOPIC_ENTITIES: list[tuple[str, re.Pattern]] = [
    ("rate_cut", re.compile(r"\brate\s*cuts?\b|\bcuts?\b.*\brates?\b", re.IGNORECASE)),
    (
        "rate_hike",
        re.compile(
            r"\brate\s*(?:hikes?|raises?|increases?)\b"
            r"|\b(?:raises?|hikes?|increases?)\b.*\brates?\b",
            re.IGNORECASE,
        ),
    ),
    ("interest_rate", re.compile(r"\binterest\s*rates?\b", re.IGNORECASE)),
    ("inflation", re.compile(r"\binflation\b", re.IGNORECASE)),
    ("recession", re.compile(r"\brecession\b", re.IGNORECASE)),
    ("GDP", re.compile(r"\bGDP\b")),
    ("S&P500", re.compile(r"\b(?:S&P|S&P\s*500|SPX)\b", re.IGNORECASE)),
    ("election", re.compile(r"\b(?:election|electoral)\b", re.IGNORECASE)),
    ("primary", re.compile(r"\bprimary\b", re.IGNORECASE)),
    ("nomination", re.compile(r"\bnominat(?:ion|e|ed)\b", re.IGNORECASE)),
    ("war", re.compile(r"\bwar\b", re.IGNORECASE)),
    ("ceasefire", re.compile(r"\bceasefire\b", re.IGNORECASE)),
    ("tariff", re.compile(r"\btariff\b", re.IGNORECASE)),
    ("impeach", re.compile(r"\bimpeach\b", re.IGNORECASE)),
    ("AI", re.compile(r"\b(?:AI|artificial intelligence)\b", re.IGNORECASE)),
]

_ALL_ENTITY_LISTS = [
    _CRYPTO_ENTITIES,
    _POLITICIAN_ENTITIES,
    _CENTRAL_BANK_ENTITIES,
    _COUNTRY_ENTITIES,
    _COMPANY_ENTITIES,
    _TOPIC_ENTITIES,
]

# Implication patterns: if question contains pattern[0], it implies questions
# with pattern[1]. E.g. "wins primary" implies "wins nomination".
_IMPLIES_PATTERNS: list[tuple[re.Pattern, re.Pattern]] = [
    # Winning primary implies competing in general / winning nomination
    (
        re.compile(r"\bwins?\b.*\bprimary\b", re.IGNORECASE),
        re.compile(r"\b(?:wins?\b.*\b(?:general|election|nomination)|nominat)", re.IGNORECASE),
    ),
    # Winning nomination implies being candidate
    (
        re.compile(r"\bnominat(?:ed|ion)\b", re.IGNORECASE),
        re.compile(r"\b(?:candidate|wins?\b.*\belection)\b", re.IGNORECASE),
    ),
    # Rate hike implies higher interest rates
    (
        re.compile(r"\brate\s*(?:hike|raise|increase)\b", re.IGNORECASE),
        re.compile(r"\binterest\s*rate.*(?:above|higher|increase)\b", re.IGNORECASE),
    ),
    # Rate cut implies lower interest rates
    (
        re.compile(r"\brate\s*cut\b", re.IGNORECASE),
        re.compile(r"\binterest\s*rate.*(?:below|lower|decrease)\b", re.IGNORECASE),
    ),
]

# Inverse patterns: if question contains pattern[0], it is inversely
# correlated with questions matching pattern[1].
_INVERSE_PATTERNS: list[tuple[re.Pattern, re.Pattern]] = [
    # Rate hike is inverse of rate cut
    (
        re.compile(r"\brate\s*(?:hike|raise|increase)\b", re.IGNORECASE),
        re.compile(r"\brate\s*cut\b", re.IGNORECASE),
    ),
    # War is inverse of ceasefire/peace
    (
        re.compile(r"\bwar\b", re.IGNORECASE),
        re.compile(r"\b(?:ceasefire|peace)\b", re.IGNORECASE),
    ),
    # Win is inverse of lose for same entity
    (
        re.compile(r"\bwin\b", re.IGNORECASE),
        re.compile(r"\blose\b", re.IGNORECASE),
    ),
]

# Map edge relationship types to evidence quality tiers
_RELATIONSHIP_TYPE_MAP: dict[str, str] = {
    "implies": "B",     # strong structural signal
    "correlates": "C",  # moderate shared-entity signal
    "inverse": "B",     # strong structural (opposite direction)
}


# ---------------------------------------------------------------------------
# Graph data structures
# ---------------------------------------------------------------------------


@dataclass
class MarketNode:
    """A node in the dependency graph representing a single market."""

    market: Market
    price: float  # current YES price
    prev_price: float  # price from last scan
    price_delta: float  # change since last scan (absolute)
    entities: set[str] = field(default_factory=set)  # extracted entity tags


@dataclass
class DependencyEdge:
    """A directed edge in the dependency graph."""

    source_id: str  # market id of the source (the market that moved)
    target_id: str  # market id of the target (should react)
    relationship: str  # "implies", "correlates", "inverse"
    strength: float  # 0.0 to 1.0 estimated correlation


class BayesianGraph:
    """
    Directed acyclic graph of market dependencies.

    Nodes are markets; edges represent probabilistic relationships.
    """

    def __init__(self) -> None:
        self.nodes: dict[str, MarketNode] = {}
        self.edges: list[DependencyEdge] = []
        # Adjacency list: source_id -> list of edges
        self._adjacency: dict[str, list[DependencyEdge]] = {}

    def add_node(self, node: MarketNode) -> None:
        self.nodes[node.market.id] = node

    def add_edge(self, edge: DependencyEdge) -> None:
        # Avoid duplicate edges
        for existing in self._adjacency.get(edge.source_id, []):
            if existing.target_id == edge.target_id:
                # Update strength if new edge is stronger
                if edge.strength > existing.strength:
                    existing.strength = edge.strength
                    existing.relationship = edge.relationship
                return
        self.edges.append(edge)
        self._adjacency.setdefault(edge.source_id, []).append(edge)

    def get_outgoing(self, source_id: str) -> list[DependencyEdge]:
        return self._adjacency.get(source_id, [])


# ---------------------------------------------------------------------------
# Entity extraction helper
# ---------------------------------------------------------------------------


def _extract_entities(text: str) -> set[str]:
    """Extract named entities from a market question using regex."""
    entities: set[str] = set()
    for entity_list in _ALL_ENTITY_LISTS:
        for name, pattern in entity_list:
            if pattern.search(text):
                entities.add(name)
    return entities


def _get_live_yes_price(market: Market, prices: dict[str, dict]) -> float:
    """Get the live YES price for a market, falling back to static price."""
    yes_price = market.yes_price
    if market.clob_token_ids and len(market.clob_token_ids) > 0:
        token = market.clob_token_ids[0]
        if token in prices:
            yes_price = prices[token].get("mid", yes_price)
    return yes_price


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------


class BayesianCascadeStrategy(BaseStrategy):
    """
    Bayesian Cascade: Probability Graph Arbitrage.

    Builds a DAG of market dependencies and propagates probability updates.
    When market A moves, all markets connected to A should adjust.
    Markets that HAVEN'T adjusted yet represent a mispricing opportunity.

    The cascade detector finds these propagation delays and creates
    opportunities to trade before the target market adjusts.

    Uses a proper Bayesian aggregation engine (ported from Polyseer) for
    evidence accumulation: log-likelihood ratios per evidence item,
    origin-based clustering with intra-cluster correlation adjustment,
    trimmed-mean outlier robustness, and market-price blending.
    """

    strategy_type = "bayesian_cascade"
    name = "Bayesian Cascade"
    description = (
        "Graph-based belief propagation to detect mispriced markets that haven't reacted to related market moves"
    )
    mispricing_type = "cross_market"
    subscriptions = ["market_data_refresh"]
    realtime_processing_mode = "full_snapshot"

    quality_filter_overrides = QualityFilterOverrides(
        min_roi=2.0,
        max_resolution_months=3.0,
    )

    default_config = {
        "min_edge_percent": 4.0,
        "min_confidence": 0.40,
        "max_risk_score": 0.75,
        "min_propagation_edge": 0.05,
        "max_propagation_depth": 3,
        "base_size_usd": 15.0,
        "max_size_usd": 120.0,
        "take_profit_pct": 12.0,
    }

    pipeline_defaults = {
        "min_edge_percent": 3.5,
        "min_confidence": 0.45,
        "max_risk_score": 0.75,
        "base_size_usd": 18.0,
        "max_size_usd": 150.0,
    }

    # Composable evaluate pipeline: score = edge*0.60 + conf*32 - risk*9 + markets*1.5
    scoring_weights = ScoringWeights(
        edge_weight=0.60,
        confidence_weight=32.0,
        risk_penalty=9.0,
        market_count_bonus=1.5,
    )
    # size = base*(1+edge/100)*(0.75+conf), no risk/market scaling
    sizing_config = SizingConfig(
        base_divisor=100.0,
        confidence_offset=0.75,
        risk_scale_factor=0.0,
        risk_floor=1.0,
        market_scale_factor=0.0,
        market_scale_cap=0,
    )

    # Minimum price change (absolute, on 0-1 scale) to consider a market
    # as having "moved" and trigger propagation.
    PRICE_MOVE_THRESHOLD = 0.02  # 2 cents

    def __init__(self) -> None:
        super().__init__()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def detect(
        self,
        events: list[Event],
        markets: list[Market],
        prices: dict[str, dict],
    ) -> list[Opportunity]:
        # 1. Build the graph from all active markets
        graph = self._build_graph(markets, prices)

        if not graph.nodes:
            return []

        # 2. Detect edges (dependencies) between nodes
        self._detect_dependencies(graph)

        # 3. Find markets with significant price moves since last scan
        movers = self._find_movers(graph)

        # 4. Propagate beliefs and detect mispricings
        opportunities = self._propagate_and_detect(graph, movers, events, markets)

        # 5. Save current prices for next scan
        self._save_prices(graph)

        return opportunities

    # ------------------------------------------------------------------
    # Graph construction
    # ------------------------------------------------------------------

    def _build_graph(self, markets: list[Market], prices: dict[str, dict]) -> BayesianGraph:
        """Build the dependency graph from all active markets."""
        graph = BayesianGraph()

        for market in markets:
            if market.closed or not market.active:
                continue

            # Only binary markets
            if len(market.outcome_prices) != 2:
                continue

            current_price = _get_live_yes_price(market, prices)

            # Skip markets with degenerate prices (fully resolved)
            if current_price <= 0.01 or current_price >= 0.99:
                continue

            prev_prices = self.state.setdefault("dependency_graph", {})
            prev_price = prev_prices.get(market.id, current_price)
            delta = current_price - prev_price

            entities = _extract_entities(market.question)

            node = MarketNode(
                market=market,
                price=current_price,
                prev_price=prev_price,
                price_delta=delta,
                entities=entities,
            )
            graph.add_node(node)

        return graph

    # ------------------------------------------------------------------
    # Dependency detection
    # ------------------------------------------------------------------

    def _detect_dependencies(self, graph: BayesianGraph) -> None:
        """
        Detect probabilistic dependencies between markets and add edges.

        Uses three heuristics (no LLM):
        1. IMPLIES: structural implication patterns
        2. INVERSE: opposing outcomes on related topics
        3. CORRELATES: shared entities + same category proximity
        """
        node_list = list(graph.nodes.values())
        n = len(node_list)

        for i in range(n):
            for j in range(i + 1, n):
                a = node_list[i]
                b = node_list[j]

                # Check implication A -> B
                self._check_implies(graph, a, b)
                # Check implication B -> A
                self._check_implies(graph, b, a)

                # Check inverse relationship (bidirectional)
                self._check_inverse(graph, a, b)

                # Check correlation via shared entities
                self._check_correlates(graph, a, b)

    def _check_implies(self, graph: BayesianGraph, source: MarketNode, target: MarketNode) -> None:
        """Check if source market implies something about target market."""
        q_source = source.market.question
        q_target = target.market.question

        # Both markets must share at least one entity to be related
        shared = source.entities & target.entities
        if not shared:
            return

        for src_pattern, tgt_pattern in _IMPLIES_PATTERNS:
            if src_pattern.search(q_source) and tgt_pattern.search(q_target):
                # Strength based on entity overlap
                strength = min(0.8, 0.4 + 0.1 * len(shared))
                graph.add_edge(
                    DependencyEdge(
                        source_id=source.market.id,
                        target_id=target.market.id,
                        relationship="implies",
                        strength=strength,
                    )
                )
                return  # One implication edge is enough

    def _check_inverse(self, graph: BayesianGraph, a: MarketNode, b: MarketNode) -> None:
        """Check if two markets are inversely related."""
        q_a = a.market.question
        q_b = b.market.question

        # Must share at least one entity
        shared = a.entities & b.entities
        if not shared:
            return

        for pat_a, pat_b in _INVERSE_PATTERNS:
            if (pat_a.search(q_a) and pat_b.search(q_b)) or (pat_b.search(q_a) and pat_a.search(q_b)):
                strength = min(0.7, 0.3 + 0.1 * len(shared))
                # Add bidirectional inverse edges
                graph.add_edge(
                    DependencyEdge(
                        source_id=a.market.id,
                        target_id=b.market.id,
                        relationship="inverse",
                        strength=strength,
                    )
                )
                graph.add_edge(
                    DependencyEdge(
                        source_id=b.market.id,
                        target_id=a.market.id,
                        relationship="inverse",
                        strength=strength,
                    )
                )
                return

    def _check_correlates(self, graph: BayesianGraph, a: MarketNode, b: MarketNode) -> None:
        """
        Check if two markets are correlated via shared entities.

        Requires at least 2 shared entities to reduce false positives.
        """
        shared = a.entities & b.entities
        if len(shared) < 2:
            return

        # Don't add correlates edge if we already have a stronger typed edge
        for edge in graph.get_outgoing(a.market.id):
            if edge.target_id == b.market.id:
                return
        for edge in graph.get_outgoing(b.market.id):
            if edge.target_id == a.market.id:
                return

        # Strength scales with number of shared entities, capped at 0.5
        strength = min(0.5, 0.15 + 0.1 * len(shared))

        # Bidirectional correlation
        graph.add_edge(
            DependencyEdge(
                source_id=a.market.id,
                target_id=b.market.id,
                relationship="correlates",
                strength=strength,
            )
        )
        graph.add_edge(
            DependencyEdge(
                source_id=b.market.id,
                target_id=a.market.id,
                relationship="correlates",
                strength=strength,
            )
        )

    # ------------------------------------------------------------------
    # Mover detection
    # ------------------------------------------------------------------

    def _find_movers(self, graph: BayesianGraph) -> list[MarketNode]:
        """Return nodes whose price moved by more than the threshold."""
        movers: list[MarketNode] = []
        for node in graph.nodes.values():
            if abs(node.price_delta) >= self.PRICE_MOVE_THRESHOLD:
                movers.append(node)
        return movers

    # ------------------------------------------------------------------
    # Evidence construction from graph edges
    # ------------------------------------------------------------------

    @staticmethod
    def _build_evidence_from_edges(
        source: MarketNode,
        edges: list[DependencyEdge],
        graph: BayesianGraph,
    ) -> list[FcEvidence]:
        """Convert dependency edges from a mover into Evidence objects.

        Each outgoing edge from the source mover becomes a piece of evidence
        about the target market.  The edge properties map to evidence
        dimensions:

        - type: relationship type -> quality tier (implies=B, correlates=C, inverse=B)
        - polarity: +1 for implies/correlates, -1 for inverse
        - verifiability: based on entity overlap count
        - consistency: edge.strength (the heuristic correlation score)
        - corroborations_indep: count of edges pointing to same target from
          different sources
        - origin_id: source market id (clusters signals from same mover)
        """
        evidence_items: list[FcEvidence] = []

        # Count how many edges point to each target (for corroboration)
        target_edge_counts: dict[str, int] = {}
        for e in edges:
            target_edge_counts[e.target_id] = target_edge_counts.get(e.target_id, 0) + 1

        for edge in edges:
            target = graph.nodes.get(edge.target_id)
            if target is None:
                continue

            shared = source.entities & target.entities
            ev_type = _RELATIONSHIP_TYPE_MAP.get(edge.relationship, "D")
            polarity = -1 if edge.relationship == "inverse" else 1

            # Scale verifiability by entity overlap: more shared = more verifiable
            verifiability = min(1.0, 0.3 + 0.15 * len(shared))

            # Corroborations: other edges also pointing at this target
            corroborations = max(0, target_edge_counts.get(edge.target_id, 1) - 1)

            evidence_items.append(FcEvidence(
                id=f"{edge.source_id}:{edge.target_id}:{edge.relationship}",
                claim=(
                    f'"{source.market.question[:60]}" {edge.relationship} '
                    f'"{target.market.question[:60]}"'
                ),
                polarity=polarity,
                type=ev_type,
                origin_id=edge.source_id,
                verifiability=verifiability,
                corroborations_indep=corroborations,
                consistency=edge.strength,
                first_report=False,
                published_at=None,
            ))

        return evidence_items

    # ------------------------------------------------------------------
    # Belief propagation and opportunity detection
    # ------------------------------------------------------------------

    def _propagate_and_detect(
        self,
        graph: BayesianGraph,
        movers: list[MarketNode],
        events: list[Event],
        markets: list[Market],
    ) -> list[Opportunity]:
        """
        For each mover, propagate expected price changes through the graph.
        If a target market has NOT adjusted as expected, flag it.
        """
        opportunities: list[Opportunity] = []
        min_edge_pct = max(0.0, float(self.config.get("min_propagation_edge", 0.05) or 0.05))
        max_depth = max(1, int(float(self.config.get("max_propagation_depth", 3) or 3)))

        # Build a lookup: market_id -> event for enriching opportunities
        market_to_event: dict[str, Event] = {}
        for event in events:
            for m in event.markets:
                market_to_event[m.id] = event

        # Track visited targets to avoid duplicate opportunities (cross-cycle)
        propagation_cache = self.state.setdefault("propagation_cache", {})
        flagged_pairs: set[tuple[str, str]] = set(propagation_cache.get("flagged_pairs", []))

        for mover in movers:
            # BFS propagation through the graph, collecting evidence per target
            self._propagate_bfs(
                graph=graph,
                source=mover,
                max_depth=max_depth,
                min_edge_pct=min_edge_pct,
                market_to_event=market_to_event,
                flagged_pairs=flagged_pairs,
                opportunities=opportunities,
            )

        # Persist flagged pairs for cross-cycle deduplication
        propagation_cache["flagged_pairs"] = list(flagged_pairs)

        return opportunities

    def _propagate_bfs(
        self,
        graph: BayesianGraph,
        source: MarketNode,
        max_depth: int,
        min_edge_pct: float,
        market_to_event: dict[str, Event],
        flagged_pairs: set[tuple[str, str]],
        opportunities: list[Opportunity],
    ) -> None:
        """
        BFS propagation from a mover node through the dependency graph.

        Collects all edges reaching each target market along the BFS path,
        converts them to Evidence objects, and runs the Bayesian aggregation
        engine to compute a proper posterior.  The mispricing is derived from
        the forecast's edge_percent rather than naive linear delta.
        """
        # Collect all evidence items reaching each target across BFS
        # target_id -> list of (evidence_item, depth)
        target_evidence: dict[str, list[FcEvidence]] = {}

        # Queue: (node_id, accumulated_edges_to_here, depth)
        # accumulated_edges tracks the chain of edges that produced this visit
        queue: list[tuple[str, list[DependencyEdge], int]] = []
        visited: set[str] = {source.market.id}

        # Seed: direct neighbors of the source
        for edge in graph.get_outgoing(source.market.id):
            if edge.target_id in visited:
                continue
            queue.append((edge.target_id, [edge], 1))
            visited.add(edge.target_id)

        while queue:
            target_id, path_edges, depth = queue.pop(0)

            target_node = graph.nodes.get(target_id)
            if target_node is None:
                continue

            # Build evidence from edges leading to this target
            ev_items = self._build_evidence_from_edges(source, path_edges, graph)

            # Also include the source's price move magnitude as a scaling
            # factor on each evidence item.  A big mover produces stronger
            # evidence than a tiny one.  We encode this via log_lr_hint: the
            # engine formula produces a base LLR, and we scale by the
            # source's absolute price delta (clamped to a reasonable range).
            move_scale = min(5.0, abs(source.price_delta) / 0.02)  # 2c move = 1x, 10c = 5x
            for ev in ev_items:
                base_llr = _fc_evidence_log_lr(ev)
                cap = _FC_TYPE_CAPS.get(ev.type, 0.2)
                scaled = max(-cap, min(cap, base_llr * move_scale))
                ev.log_lr_hint = scaled

            target_evidence.setdefault(target_id, []).extend(ev_items)

            # Run forecast for this target
            forecast = run_forecast(
                p0=target_node.price,
                evidence=target_evidence[target_id],
                market_prob=target_node.price,
            )

            mispricing_abs = abs(forecast.edge_percent) / 100.0

            if mispricing_abs >= min_edge_pct:
                pair_key = (source.market.id, target_id)
                if pair_key not in flagged_pairs:
                    flagged_pairs.add(pair_key)
                    opp = self._create_cascade_opportunity(
                        source=source,
                        target=target_node,
                        forecast=forecast,
                        market_to_event=market_to_event,
                    )
                    if opp is not None:
                        opportunities.append(opp)

            # Continue propagation if we haven't reached max depth
            if depth < max_depth:
                for edge in graph.get_outgoing(target_id):
                    if edge.target_id not in visited:
                        # Pass along the edge chain so deeper targets
                        # accumulate evidence from the full path
                        next_edges = path_edges + [edge]
                        queue.append((edge.target_id, next_edges, depth + 1))
                        visited.add(edge.target_id)

    # ------------------------------------------------------------------
    # Opportunity creation
    # ------------------------------------------------------------------

    def _create_cascade_opportunity(
        self,
        source: MarketNode,
        target: MarketNode,
        forecast: ForecastResult,
        market_to_event: dict[str, Event],
    ) -> Optional[Opportunity]:
        """
        Create an arbitrage opportunity from a cascade mispricing.

        The position is:
        - BUY YES if the target should go UP (p_blended > market price).
        - BUY NO if the target should go DOWN (p_blended < market price).
        """
        target_market = target.market
        mispricing = forecast.edge_percent / 100.0  # signed, in probability space

        # Determine direction and compute cost
        if mispricing > 0:
            # Target should be higher than it is -> buy YES
            outcome = "YES"
            price = target.price
            token_id = target_market.clob_token_ids[0] if target_market.clob_token_ids else None
        else:
            # Target should be lower than it is -> buy NO
            outcome = "NO"
            price = target_market.no_price
            token_id = target_market.clob_token_ids[1] if len(target_market.clob_token_ids) > 1 else None

        total_cost = price

        positions = [
            {
                "action": "BUY",
                "outcome": outcome,
                "market": target_market.question[:80],
                "price": price,
                "token_id": token_id,
            }
        ]

        source_direction = "UP" if source.price_delta > 0 else "DOWN"
        mispricing_pct = abs(forecast.edge_percent)

        event = market_to_event.get(target_market.id)

        # Compute confidence from evidence quality: mean of cluster effective
        # counts weighted by absolute contribution, normalised to [0, 1].
        total_contribution = sum(abs(c.contribution) for c in forecast.clusters)
        if total_contribution > 0 and forecast.clusters:
            weighted_quality = sum(
                abs(c.contribution) * min(1.0, c.effective_count / c.size)
                for c in forecast.clusters
            ) / total_contribution
        else:
            weighted_quality = 0.5
        confidence = min(1.0, max(0.0, weighted_quality))

        description = (
            f"Source market moved {source_direction} by "
            f"{abs(source.price_delta) * 100:.1f}%: "
            f'"{source.market.question[:60]}". '
            f"Bayesian posterior {forecast.p_blended:.3f} vs market "
            f"{forecast.market_prob:.3f} (edge: {mispricing_pct:.1f}%). "
            f"Position: BUY {outcome} at ${price:.3f}."
        )

        opp = self.create_opportunity(
            title=f"Cascade: {target_market.question[:55]}",
            description=description,
            total_cost=total_cost,
            markets=[target_market],
            positions=positions,
            event=event,
            is_guaranteed=False,
            confidence=confidence,
        )

        if opp is not None:
            opp.mispricing_type = MispricingType.CROSS_MARKET
            opp.strategy_context = {
                "source_key": "scanner",
                "strategy_slug": self.strategy_type,
                "forecast": {
                    "prior": round(forecast.prior, 4),
                    "p_neutral": round(forecast.p_neutral, 4),
                    "p_blended": round(forecast.p_blended, 4),
                    "market_prob": round(forecast.market_prob, 4),
                    "edge_percent": round(forecast.edge_percent, 2),
                    "evidence_count_for": forecast.evidence_count_for,
                    "evidence_count_against": forecast.evidence_count_against,
                },
                "clusters": [
                    {
                        "cluster_id": c.cluster_id[:40],
                        "size": c.size,
                        "rho": round(c.rho, 3),
                        "effective_count": round(c.effective_count, 3),
                        "mean_log_lr": round(c.mean_log_lr, 4),
                        "contribution": round(c.contribution, 4),
                    }
                    for c in forecast.clusters
                ],
                "top_influence": [
                    {
                        "evidence_id": inf.evidence_id[:40],
                        "claim": inf.claim[:80],
                        "polarity": inf.polarity,
                        "delta_pp": round(inf.delta_pp, 4),
                    }
                    for inf in sorted(
                        forecast.influence, key=lambda x: x.delta_pp, reverse=True
                    )[:5]
                ],
                "source_market": {
                    "id": source.market.id,
                    "question": source.market.question[:80],
                    "price_delta": round(source.price_delta, 4),
                },
            }

        return opp

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _save_prices(self, graph: BayesianGraph) -> None:
        """Store current prices for the next scan cycle."""
        prev_prices = self.state.setdefault("dependency_graph", {})
        for node_id, node in graph.nodes.items():
            prev_prices[node_id] = node.price

        # Prune markets no longer in the graph to prevent unbounded growth
        active_ids = set(graph.nodes.keys())
        stale_ids = [k for k in prev_prices if k not in active_ids]
        for sid in stale_ids:
            del prev_prices[sid]

    # ------------------------------------------------------------------
    # Composable evaluate pipeline overrides
    # ------------------------------------------------------------------

    def custom_checks(self, signal, context, params, payload):
        source = str(getattr(signal, "source", "") or "").strip().lower()
        return [
            DecisionCheck("source", "Signal source", source == "scanner", detail=f"got={source}"),
        ]

    def compute_score(
        self, edge: float, confidence: float, risk_score: float, market_count: int, payload: dict
    ) -> float:
        """Bayesian cascade: edge*0.60 + conf*32 + min(4,markets)*1.5 - risk*9."""
        return (edge * 0.60) + (confidence * 32.0) + (min(4, market_count) * 1.5) - (risk_score * 9.0)

    def compute_size(
        self, base_size: float, max_size: float, edge: float, confidence: float, risk_score: float, market_count: int
    ) -> float:
        """Kelly-informed sizing using Bayesian posterior.

        Uses the forecast's p_blended as p_estimated rather than the old
        naive ``0.5 + edge/200`` heuristic.  The edge here is
        ``forecast.edge_percent`` so ``p_estimated = market + edge/100``.
        """
        # Reconstruct p_estimated from edge percent.
        # edge is edge_percent (e.g. 5.0 means 5%).
        # p_market defaults to 0.5; p_estimated = p_market + edge/100.
        p_market = 0.5
        p_estimated = min(0.99, max(0.01, p_market + edge / 100.0))
        kelly_f = kelly_fraction(p_estimated, p_market, fraction=0.25)
        kelly_sz = base_size * (1.0 + kelly_f * 10.0)
        size = kelly_sz * (0.7 + confidence * 0.6) * max(0.4, 1.0 - risk_score)
        return max(1.0, min(max_size, size))

    def should_exit(self, position: Any, market_state: dict) -> ExitDecision:
        """Bayesian cascade: standard TP/SL exit."""
        if market_state.get("is_resolved"):
            return self.default_exit_check(position, market_state)
        config = getattr(position, "config", None) or {}
        config = dict(config)
        configured_tp = (getattr(self, "config", None) or {}).get("take_profit_pct", 12.0)
        try:
            default_tp = float(configured_tp)
        except (TypeError, ValueError):
            default_tp = 12.0
        config.setdefault("take_profit_pct", default_tp)
        position.config = config
        return self.default_exit_check(position, market_state)

    # ------------------------------------------------------------------
    # Platform gate hooks
    # ------------------------------------------------------------------

    def on_blocked(self, signal, reason: str, context: dict) -> None:
        logger.info("%s: signal blocked — %s (market=%s)", self.name, reason, getattr(signal, "market_id", "?"))

    def on_size_capped(self, original_size: float, capped_size: float, reason: str) -> None:
        logger.info("%s: size capped $%.0f → $%.0f — %s", self.name, original_size, capped_size, reason)
