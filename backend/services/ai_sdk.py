"""Unified AI SDK for strategies, data sources, and trader intelligence."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from services.ai.llm_provider import LLMManager

from utils.logger import get_logger

logger = get_logger(__name__)


class AISDK:
    """Unified AI capabilities available to strategies, data sources, and custom agents."""

    def __init__(self, llm_manager: LLMManager, purpose: str = "sdk"):
        self._llm = llm_manager
        self._purpose = purpose

    async def chat(self, prompt: str, *, system: str = None, model: str = None, temperature: float = 0.0) -> str:
        """Send a prompt, get text response."""
        from services.ai.llm_provider import LLMMessage

        messages = []
        if system:
            messages.append(LLMMessage(role="system", content=system))
        messages.append(LLMMessage(role="user", content=prompt))

        response = await self._llm.chat(
            messages=messages,
            model=model,
            temperature=temperature,
            purpose=self._purpose,
        )
        return response.content

    async def chat_json(
        self, prompt: str, schema: dict, *, system: str = None, model: str = None, temperature: float = 0.0
    ) -> dict:
        """Send a prompt, get structured JSON response validated against schema."""
        from services.ai.llm_provider import LLMMessage

        messages = []
        if system:
            messages.append(LLMMessage(role="system", content=system))
        messages.append(LLMMessage(role="user", content=prompt))

        return await self._llm.structured_output(
            messages=messages,
            schema=schema,
            model=model,
            temperature=temperature,
            purpose=self._purpose,
        )

    async def chat_messages(self, messages: list[dict], *, model: str = None, temperature: float = 0.0) -> str:
        """Send full message history, get text response."""
        from services.ai.llm_provider import LLMMessage

        llm_messages = [LLMMessage(role=m["role"], content=m["content"]) for m in messages]

        response = await self._llm.chat(
            messages=llm_messages,
            model=model,
            temperature=temperature,
            purpose=self._purpose,
        )
        return response.content

    async def analyze_resolution(self, market_id: str, question: str, **kwargs) -> dict:
        """Run resolution criteria analysis. Returns clarity_score, risk_score, recommendation, etc."""
        from services.ai.resolution_analyzer import resolution_analyzer

        return await resolution_analyzer.analyze_market(
            market_id=market_id,
            question=question,
            **kwargs,
        )

    async def analyze_sentiment(self, query: str, *, max_articles: int = 5) -> dict:
        """Run news sentiment analysis. Returns sentiment_score, confidence, key_takeaways, etc."""
        from services.ai.news_sentiment import news_sentiment_analyzer

        return await news_sentiment_analyzer.search_and_analyze(
            query=query,
            max_articles=max_articles,
        )

    async def judge_opportunity(self, opportunity_id: str) -> dict:
        """Run LLM-as-judge on an opportunity. Returns scores and recommendation."""
        from services.ai.opportunity_judge import opportunity_judge
        from services import shared_state
        from services.weather import shared_state as weather_shared_state
        from models.database import AsyncSessionLocal

        async with AsyncSessionLocal() as session:
            scanner_opps = await shared_state.get_opportunities_from_db(session, None)
            opp = next((o for o in scanner_opps if o.id == opportunity_id), None)

            if opp is None:
                weather_opps = await weather_shared_state.get_weather_opportunities_from_db(
                    session, include_report_only=True
                )
                opp = next((o for o in weather_opps if o.id == opportunity_id), None)

        if opp is None:
            return {"error": f"Opportunity {opportunity_id} not found"}

        return await opportunity_judge.judge_opportunity(opportunity=opp)

    async def run_agent(
        self,
        agent_name_or_prompt: str,
        query: str,
        *,
        tools: list = None,
        model: str = None,
        max_iterations: int = 10,
    ) -> dict:
        """Run a ReAct agent. Returns final result dict."""
        from services.ai.agent import run_agent_to_completion

        return await run_agent_to_completion(
            system_prompt=agent_name_or_prompt,
            query=query,
            tools=tools,
            model=model,
            max_iterations=max_iterations,
            session_type=self._purpose,
        )

    async def run_skill(self, skill_name: str, query: str, *, model: str = None) -> dict:
        """Execute a named skill. Returns skill result."""
        from services.ai.skills.loader import skill_loader

        return await skill_loader.execute_skill(
            name=skill_name,
            context={"query": query},
            model=model,
        )
