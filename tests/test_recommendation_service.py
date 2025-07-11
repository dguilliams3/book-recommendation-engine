import json, sys, types

import pytest
from langchain_core.messages import AIMessage
from unittest.mock import AsyncMock, patch
from src.recommendation_api import service as svc

# ---------------------------------------------------------------------------
# Autouse fixture: stub heavy external dependencies BEFORE importing the service
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _patch_recommendation_deps(monkeypatch):
    """Create lightweight stub modules so `recommendation_api.service` can import."""

    # ---- stub `mcp` ------------------------------------------------------
    dummy_mcp = types.ModuleType("mcp")

    def _dummy_stdio(_params):
        class _Ctx:
            async def __aenter__(self):
                return (None, None)  # read, write placeholders

            async def __aexit__(self, exc_type, exc, tb):
                return False

        return _Ctx()

    class _DummySession:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def initialize(self):
            return None

    # simple passthrough object for server params
    class _DummyParams:
        def __init__(self, *args, **kwargs):
            pass

    dummy_mcp.stdio_client = _dummy_stdio
    dummy_mcp.ClientSession = _DummySession
    dummy_mcp.StdioServerParameters = _DummyParams

    monkeypatch.setitem(sys.modules, "mcp", dummy_mcp)

    # ---- stub langchain_mcp_adapters.tools.load_mcp_tools ---------------
    tools_mod = types.ModuleType("langchain_mcp_adapters.tools")

    async def _load_mcp_tools(_session):
        return []

    tools_mod.load_mcp_tools = _load_mcp_tools
    monkeypatch.setitem(sys.modules, "langchain_mcp_adapters.tools", tools_mod)

    # ---- stub langgraph.prebuilt.create_react_agent ----------------------
    lg_prebuilt = types.ModuleType("langgraph.prebuilt")
    lg_prebuilt.create_react_agent = lambda _model, _tools, name="x": object()
    monkeypatch.setitem(sys.modules, "langgraph.prebuilt", lg_prebuilt)


class DummyParser:
    def __init__(self, result=None, raise_exc=False):
        self._result = result
        self._raise_exc = raise_exc
    def parse(self, msg):
        if self._raise_exc:
            raise Exception("Not JSON")
        return self._result


def test_generate_recommendations_parses_json(monkeypatch):
    sample_json = json.dumps([
        {
            "book_id": "b1",
            "title": "Cat Stories",
            "librarian_blurb": "Cute cat tales."
        },
        {
            "book_id": "b2",
            "title": "Dog Adventures",
            "librarian_blurb": "Exciting dog journeys."
        }
    ])

    async def _fake_ask(_agent, _prompt, callbacks=None):
        if callbacks:
            for cb in callbacks:
                cb.tools_used.extend(["dummy_tool"])
        return AIMessage(content=sample_json), {}

    monkeypatch.setattr(svc, "_ask_agent", _fake_ask)
    monkeypatch.setattr(svc, "_get_student_context_cached", AsyncMock(return_value=(4.0, ["Book1"], ["Fiction"], {})))
    monkeypatch.setattr(svc, "build_candidates", AsyncMock(return_value=[svc.BookRecommendation(book_id="b1", title="Cat Stories", author="", reading_level=4.0, librarian_blurb="", justification=""), svc.BookRecommendation(book_id="b2", title="Dog Adventures", author="", reading_level=4.0, librarian_blurb="", justification="")]))
    monkeypatch.setattr(svc, "score_candidates", lambda c, s, q: c)
    monkeypatch.setattr(svc, "_get_fallback_recommendations", AsyncMock(return_value=[]))
    monkeypatch.setattr(svc, "mark_recommended", AsyncMock())
    monkeypatch.setattr(svc, "parser", DummyParser([
        svc.BookRecommendation(book_id="b1", title="Cat Stories", author="", reading_level=4.0, librarian_blurb="Cute cat tales.", justification=""),
        svc.BookRecommendation(book_id="b2", title="Dog Adventures", author="", reading_level=4.0, librarian_blurb="Exciting dog journeys.", justification="")
    ]))

    import asyncio
    recs, meta = asyncio.run(svc.generate_recommendations("stu1", "cats", 2, "req1"))

    assert len(recs) == 2
    assert recs[0].book_id == "b1"
    assert meta["tool_count"] == 1

def test_generate_recommendations_handles_non_json(monkeypatch):
    async def _fake_ask(_agent, _prompt, callbacks=None):
        return AIMessage(content="Some plain English answer"), {}

    monkeypatch.setattr(svc, "_ask_agent", _fake_ask)
    monkeypatch.setattr(svc, "_get_student_context_cached", AsyncMock(return_value=(4.0, ["Book1"], ["Fiction"], {})))
    monkeypatch.setattr(svc, "build_candidates", AsyncMock(return_value=[]))
    monkeypatch.setattr(svc, "score_candidates", lambda c, s, q: c)
    monkeypatch.setattr(svc, "_get_fallback_recommendations", AsyncMock(return_value=[svc.BookRecommendation(book_id="UNKNOWN", title="Unknown", author="", reading_level=0.0, librarian_blurb="Some plain English answer", justification="")]))
    monkeypatch.setattr(svc, "mark_recommended", AsyncMock())
    monkeypatch.setattr(svc, "parser", DummyParser(raise_exc=True))

    import asyncio
    recs, meta = asyncio.run(svc.generate_recommendations("stu2", "dinosaurs", 3, "req2"))

    assert len(recs) == 1
    assert recs[0].book_id == "UNKNOWN"
    assert "agent_duration" in meta 