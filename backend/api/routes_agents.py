"""
API routes for user-defined AI agents.

Provides CRUD endpoints for agent configurations and a streaming
test-run endpoint that executes an agent and returns SSE events.
"""

import json
import uuid

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from typing import Optional

from models.database import AsyncSessionLocal, UserAgent, UserTool
from services.ai.agent import Agent, AgentEventType, AgentTool
from utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/ai/agents", tags=["ai-agents"])


# ---------------------------------------------------------------------------
# Available tools registry
# ---------------------------------------------------------------------------

# Each entry describes a tool that can be assigned to a user agent.
# The handler is resolved at runtime via _resolve_tool().
AVAILABLE_TOOLS: list[dict] = [
    {
        "name": "get_market_details",
        "description": "Fetch detailed information about a Polymarket market including question, prices, volume, and liquidity.",
        "category": "market_data",
    },
    {
        "name": "search_news",
        "description": "Search for recent news articles related to a topic or market question with sentiment analysis.",
        "category": "news",
    },
    {
        "name": "analyze_resolution",
        "description": "Deep analysis of a market's resolution criteria, checking for ambiguities, edge cases, and risks.",
        "category": "analysis",
    },
    {
        "name": "check_orderbook",
        "description": "Check the order book depth and liquidity for a market's tokens.",
        "category": "market_data",
    },
    {
        "name": "find_related_markets",
        "description": "Find other Polymarket markets related to the same event or topic.",
        "category": "market_data",
    },
]


def _resolve_tools(tool_names: list[str]) -> list[AgentTool]:
    """Resolve tool name strings into AgentTool instances backed by the MarketAnalyzer."""
    from services.ai.market_analyzer import MarketAnalyzer

    analyzer = MarketAnalyzer()
    all_tools = analyzer._build_tools()
    tool_map = {t.name: t for t in all_tools}

    resolved = []
    for name in tool_names:
        tool = tool_map.get(name)
        if tool:
            resolved.append(tool)
        else:
            logger.warning("Unknown tool requested for agent: %s", name)
    return resolved


def _agent_row_to_dict(row: UserAgent) -> dict:
    return {
        "id": row.id,
        "name": row.name,
        "description": row.description or "",
        "system_prompt": row.system_prompt,
        "tools": row.tools or [],
        "model": row.model,
        "temperature": row.temperature,
        "max_iterations": row.max_iterations,
        "is_builtin": row.is_builtin,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class CreateAgentRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: str = Field(default="", max_length=500)
    system_prompt: str = Field(..., min_length=1)
    tools: list[str] = Field(default_factory=list)
    model: str | None = Field(default=None)
    temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    max_iterations: int = Field(default=10, ge=1, le=50)


class UpdateAgentRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    description: str | None = Field(default=None, max_length=500)
    system_prompt: str | None = Field(default=None, min_length=1)
    tools: list[str] | None = None
    model: str | None = None
    temperature: float | None = Field(default=None, ge=0.0, le=2.0)
    max_iterations: int | None = Field(default=None, ge=1, le=50)


class TestAgentRequest(BaseModel):
    query: str = Field(..., min_length=1)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/meta/available-tools")
async def list_available_tools() -> dict:
    """Return the list of tools that can be assigned to agents."""
    return {"tools": AVAILABLE_TOOLS}


@router.get("")
async def list_agents() -> dict:
    """List all agents (builtin + user-created)."""
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(UserAgent).order_by(UserAgent.is_builtin.desc(), UserAgent.name)
            )
            rows = result.scalars().all()
            return {"agents": [_agent_row_to_dict(r) for r in rows]}
    except OperationalError as exc:
        raise HTTPException(status_code=503, detail="Database is busy; please retry.") from exc


@router.get("/{agent_id}")
async def get_agent(agent_id: str) -> dict:
    """Get a single agent by ID."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(UserAgent).where(UserAgent.id == agent_id))
        row = result.scalar_one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail=f"Agent not found: {agent_id}")
        return _agent_row_to_dict(row)


@router.post("")
async def create_agent(request: CreateAgentRequest) -> dict:
    """Create a new user agent."""
    agent_id = uuid.uuid4().hex[:16]
    try:
        async with AsyncSessionLocal() as session:
            row = UserAgent(
                id=agent_id,
                name=request.name,
                description=request.description,
                system_prompt=request.system_prompt,
                tools=request.tools,
                model=request.model,
                temperature=request.temperature,
                max_iterations=request.max_iterations,
                is_builtin=False,
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return _agent_row_to_dict(row)
    except OperationalError as exc:
        raise HTTPException(status_code=503, detail="Database is busy; please retry.") from exc


@router.put("/{agent_id}")
async def update_agent(agent_id: str, request: UpdateAgentRequest) -> dict:
    """Update an existing agent. Builtin agents cannot be modified."""
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(UserAgent).where(UserAgent.id == agent_id))
            row = result.scalar_one_or_none()
            if not row:
                raise HTTPException(status_code=404, detail=f"Agent not found: {agent_id}")
            if row.is_builtin:
                raise HTTPException(status_code=400, detail="Cannot modify a builtin agent.")

            if request.name is not None:
                row.name = request.name
            if request.description is not None:
                row.description = request.description
            if request.system_prompt is not None:
                row.system_prompt = request.system_prompt
            if request.tools is not None:
                row.tools = request.tools
            if request.model is not None:
                row.model = request.model
            if request.temperature is not None:
                row.temperature = request.temperature
            if request.max_iterations is not None:
                row.max_iterations = request.max_iterations

            await session.commit()
            await session.refresh(row)
            return _agent_row_to_dict(row)
    except HTTPException:
        raise
    except OperationalError as exc:
        raise HTTPException(status_code=503, detail="Database is busy; please retry.") from exc


@router.delete("/{agent_id}")
async def delete_agent(agent_id: str) -> dict:
    """Delete a user agent. Builtin agents cannot be deleted."""
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(UserAgent).where(UserAgent.id == agent_id))
            row = result.scalar_one_or_none()
            if not row:
                raise HTTPException(status_code=404, detail=f"Agent not found: {agent_id}")
            if row.is_builtin:
                raise HTTPException(status_code=400, detail="Cannot delete a builtin agent.")
            await session.delete(row)
            await session.commit()
            return {"deleted": True, "id": agent_id}
    except HTTPException:
        raise
    except OperationalError as exc:
        raise HTTPException(status_code=503, detail="Database is busy; please retry.") from exc


@router.post("/{agent_id}/test")
async def test_agent(agent_id: str, request: TestAgentRequest):
    """Test-run an agent with a query, streaming results as SSE events."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(UserAgent).where(UserAgent.id == agent_id))
        row = result.scalar_one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail=f"Agent not found: {agent_id}")

        agent_config = _agent_row_to_dict(row)

    tools = _resolve_tools(agent_config["tools"])

    agent = Agent(
        system_prompt=agent_config["system_prompt"],
        tools=tools,
        model=agent_config["model"],
        max_iterations=agent_config["max_iterations"],
        session_type="user_agent_test",
        temperature=agent_config["temperature"],
    )

    async def event_stream():
        try:
            async for event in agent.run(request.query):
                payload = json.dumps({"type": event.type.value, "data": event.data}, default=str)
                yield f"data: {payload}\n\n"
                if event.type in (AgentEventType.DONE, AgentEventType.ERROR):
                    break
        except Exception as exc:
            logger.error("Agent test-run stream error: %s", exc)
            error_payload = json.dumps({"type": "error", "data": {"error": str(exc)}})
            yield f"data: {error_payload}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Tool request models
# ---------------------------------------------------------------------------


class CreateToolRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: str = Field(default="", max_length=500)
    tool_type: str = Field(default="function", max_length=50)
    parameters_schema: Optional[dict] = None
    implementation: Optional[str] = None
    enabled: bool = True


class UpdateToolRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=100)
    description: Optional[str] = Field(default=None, max_length=500)
    tool_type: Optional[str] = Field(default=None, max_length=50)
    parameters_schema: Optional[dict] = None
    implementation: Optional[str] = None
    enabled: Optional[bool] = None


def _tool_row_to_dict(row: UserTool) -> dict:
    return {
        "id": row.id,
        "name": row.name,
        "description": row.description or "",
        "tool_type": row.tool_type or "function",
        "parameters_schema": row.parameters_schema,
        "implementation": row.implementation,
        "is_builtin": row.is_builtin,
        "enabled": row.enabled,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


# ---------------------------------------------------------------------------
# Tool CRUD endpoints
# ---------------------------------------------------------------------------

tools_router = APIRouter(prefix="/ai/tools", tags=["ai-tools"])


@tools_router.get("")
async def list_tools() -> dict:
    """List all tools (builtin + user-created)."""
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(UserTool).order_by(UserTool.is_builtin.desc(), UserTool.name)
            )
            rows = result.scalars().all()
            return {"tools": [_tool_row_to_dict(r) for r in rows]}
    except OperationalError as exc:
        raise HTTPException(status_code=503, detail="Database is busy; please retry.") from exc


@tools_router.post("")
async def create_tool(request: CreateToolRequest) -> dict:
    """Create a new user tool."""
    tool_id = uuid.uuid4().hex[:16]
    try:
        async with AsyncSessionLocal() as session:
            row = UserTool(
                id=tool_id,
                name=request.name,
                description=request.description,
                tool_type=request.tool_type,
                parameters_schema=request.parameters_schema,
                implementation=request.implementation,
                is_builtin=False,
                enabled=request.enabled,
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return _tool_row_to_dict(row)
    except OperationalError as exc:
        raise HTTPException(status_code=503, detail="Database is busy; please retry.") from exc


@tools_router.put("/{tool_id}")
async def update_tool(tool_id: str, request: UpdateToolRequest) -> dict:
    """Update an existing tool. Builtin tools cannot be modified."""
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(UserTool).where(UserTool.id == tool_id))
            row = result.scalar_one_or_none()
            if not row:
                raise HTTPException(status_code=404, detail=f"Tool not found: {tool_id}")
            if row.is_builtin:
                raise HTTPException(status_code=400, detail="Cannot modify a builtin tool.")

            if request.name is not None:
                row.name = request.name
            if request.description is not None:
                row.description = request.description
            if request.tool_type is not None:
                row.tool_type = request.tool_type
            if request.parameters_schema is not None:
                row.parameters_schema = request.parameters_schema
            if request.implementation is not None:
                row.implementation = request.implementation
            if request.enabled is not None:
                row.enabled = request.enabled

            await session.commit()
            await session.refresh(row)
            return _tool_row_to_dict(row)
    except HTTPException:
        raise
    except OperationalError as exc:
        raise HTTPException(status_code=503, detail="Database is busy; please retry.") from exc


@tools_router.delete("/{tool_id}")
async def delete_tool(tool_id: str) -> dict:
    """Delete a user tool. Builtin tools cannot be deleted."""
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(UserTool).where(UserTool.id == tool_id))
            row = result.scalar_one_or_none()
            if not row:
                raise HTTPException(status_code=404, detail=f"Tool not found: {tool_id}")
            if row.is_builtin:
                raise HTTPException(status_code=400, detail="Cannot delete a builtin tool.")
            await session.delete(row)
            await session.commit()
            return {"deleted": True, "id": tool_id}
    except HTTPException:
        raise
    except OperationalError as exc:
        raise HTTPException(status_code=503, detail="Database is busy; please retry.") from exc


# ---------------------------------------------------------------------------
# Builtin agent seeding
# ---------------------------------------------------------------------------

BUILTIN_AGENTS = [
    {
        "id": "builtin_market_analyst",
        "name": "Market Analyst",
        "description": "Analyzes prediction markets by fetching market data, checking news sentiment, and evaluating resolution criteria to provide comprehensive market assessments.",
        "system_prompt": (
            "You are a Market Analyst agent for a prediction market trading platform. "
            "Your job is to analyze prediction markets and provide actionable intelligence.\n\n"
            "When given a market or topic to analyze, you should:\n"
            "1. Fetch market details to understand the question, current prices, volume, and liquidity.\n"
            "2. Search for recent news that could impact the market outcome.\n"
            "3. Evaluate the order book depth and spreads.\n"
            "4. Look for related markets that could provide cross-market signals.\n\n"
            "Provide a clear, structured analysis covering:\n"
            "- Current market state (prices, volume, liquidity)\n"
            "- News sentiment and its likely impact on the outcome\n"
            "- Liquidity assessment (is there enough to trade?)\n"
            "- Key risks and edge cases\n"
            "- Your probability estimate vs. the market price\n"
            "- A recommendation: BUY YES, BUY NO, or SKIP (with reasoning)"
        ),
        "tools": ["get_market_details", "search_news", "check_orderbook", "find_related_markets"],
        "model": None,
        "temperature": 0.0,
        "max_iterations": 10,
    },
    {
        "id": "builtin_resolution_auditor",
        "name": "Resolution Auditor",
        "description": "Audits prediction market resolution criteria for ambiguities, edge cases, and risks that could lead to unexpected outcomes.",
        "system_prompt": (
            "You are a Resolution Auditor agent for a prediction market trading platform. "
            "Your job is to deeply analyze resolution criteria and flag risks.\n\n"
            "When given a market to audit, you should:\n"
            "1. Fetch the full market details including resolution source and description.\n"
            "2. Run a deep resolution analysis to identify ambiguities and edge cases.\n"
            "3. Search for news that might create resolution ambiguity.\n\n"
            "Provide a structured audit report covering:\n"
            "- Resolution criteria summary\n"
            "- Ambiguities found (specific language that could be interpreted multiple ways)\n"
            "- Edge cases that might cause unexpected resolution\n"
            "- Historical precedents for similar resolutions\n"
            "- Risk score (0-10, where 10 is most risky)\n"
            "- Recommendation: SAFE, CAUTION, or AVOID (with reasoning)"
        ),
        "tools": ["analyze_resolution", "get_market_details", "search_news"],
        "model": None,
        "temperature": 0.0,
        "max_iterations": 10,
    },
    {
        "id": "builtin_news_sentinel",
        "name": "News Sentinel",
        "description": "Monitors breaking news and evaluates its impact on prediction market positions and upcoming resolutions.",
        "system_prompt": (
            "You are a News Sentinel agent for a prediction market trading platform. "
            "Your job is to monitor news and evaluate its impact on markets.\n\n"
            "When given a topic or market to monitor, you should:\n"
            "1. Search for the latest news on the topic.\n"
            "2. Fetch current market prices to compare against news sentiment.\n"
            "3. Identify any breaking developments that could shift probabilities.\n\n"
            "Provide a structured intelligence brief covering:\n"
            "- Latest news developments (with sources and recency)\n"
            "- Sentiment assessment (bullish, bearish, neutral)\n"
            "- Impact analysis: how this news should affect market probabilities\n"
            "- Price dislocation: is the market reacting or is there an opportunity?\n"
            "- Urgency level: IMMEDIATE, WATCH, or INFORMATIONAL"
        ),
        "tools": ["search_news", "get_market_details"],
        "model": None,
        "temperature": 0.0,
        "max_iterations": 10,
    },
    {
        "id": "builtin_strategy_tuner",
        "name": "Strategy Tuner",
        "description": "Analyzes market conditions and recommends strategy parameter adjustments for optimal trading performance.",
        "system_prompt": (
            "You are a Strategy Tuner agent for a prediction market trading platform. "
            "Your job is to analyze market conditions and recommend strategy adjustments.\n\n"
            "When asked to evaluate strategy performance or market conditions, you should:\n"
            "1. Fetch current market data to understand the trading environment.\n"
            "2. Search for news that could indicate regime changes.\n"
            "3. Check order book conditions to assess liquidity.\n\n"
            "Provide a structured tuning recommendation covering:\n"
            "- Current market regime (trending, range-bound, volatile, calm)\n"
            "- Liquidity conditions across key markets\n"
            "- Recommended parameter adjustments (with reasoning)\n"
            "- Risk considerations for the current environment\n"
            "- Confidence level in recommendations: HIGH, MEDIUM, or LOW"
        ),
        "tools": ["get_market_details", "search_news", "check_orderbook"],
        "model": None,
        "temperature": 0.0,
        "max_iterations": 10,
    },
]


BUILTIN_TOOLS = [
    {
        "id": "builtin_get_market_details",
        "name": "get_market_details",
        "description": "Fetch detailed information about a Polymarket market including question, prices, volume, and liquidity.",
        "tool_type": "function",
    },
    {
        "id": "builtin_search_news",
        "name": "search_news",
        "description": "Search for recent news articles related to a topic or market question with sentiment analysis.",
        "tool_type": "function",
    },
    {
        "id": "builtin_analyze_resolution",
        "name": "analyze_resolution",
        "description": "Deep analysis of a market's resolution criteria, checking for ambiguities, edge cases, and risks.",
        "tool_type": "function",
    },
    {
        "id": "builtin_check_orderbook",
        "name": "check_orderbook",
        "description": "Check the order book depth and liquidity for a market's tokens.",
        "tool_type": "function",
    },
    {
        "id": "builtin_find_related_markets",
        "name": "find_related_markets",
        "description": "Find other Polymarket markets related to the same event or topic.",
        "tool_type": "function",
    },
]


async def seed_builtin_agents() -> None:
    """Insert builtin agents and tools if they don't already exist. Idempotent."""
    async with AsyncSessionLocal() as session:
        for agent_def in BUILTIN_AGENTS:
            result = await session.execute(
                select(UserAgent).where(UserAgent.id == agent_def["id"])
            )
            if result.scalar_one_or_none() is not None:
                continue

            row = UserAgent(
                id=agent_def["id"],
                name=agent_def["name"],
                description=agent_def["description"],
                system_prompt=agent_def["system_prompt"],
                tools=agent_def["tools"],
                model=agent_def["model"],
                temperature=agent_def["temperature"],
                max_iterations=agent_def["max_iterations"],
                is_builtin=True,
            )
            session.add(row)
            logger.info("Seeded builtin agent: %s", agent_def["name"])

        for tool_def in BUILTIN_TOOLS:
            result = await session.execute(
                select(UserTool).where(UserTool.id == tool_def["id"])
            )
            if result.scalar_one_or_none() is not None:
                continue

            row = UserTool(
                id=tool_def["id"],
                name=tool_def["name"],
                description=tool_def["description"],
                tool_type=tool_def["tool_type"],
                is_builtin=True,
                enabled=True,
            )
            session.add(row)
            logger.info("Seeded builtin tool: %s", tool_def["name"])

        await session.commit()
