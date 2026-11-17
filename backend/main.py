import asyncio
import json
import uuid
from typing import Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from agent.brain import ClaudeBrain
from agent.session import ChatSession
from agent.tools import massive_get

app = FastAPI()
brain = ClaudeBrain()
chats: dict[str, ChatSession] = {}


class NewChat(BaseModel):
    # "serial" is the baseline: same router + workers, but one worker at a time.
    mode: Literal["parallel", "serial"] = "parallel"
    # Seconds of quiet from the asker before a new request's worker starts (0 = off).
    coalesce: float = 3.0


class Incoming(BaseModel):
    user: str
    text: str
    thread: int | None = None  # reply in the thread containing this slot


def get_chat(chat_id: str) -> ChatSession:
    """Look up a chat by id or return 404."""
    if chat_id not in chats:
        raise HTTPException(404, "unknown chat")
    return chats[chat_id]


@app.get("/api/health")
def health():
    """Liveness check."""
    return {"ok": True}


@app.post("/api/chats")
async def create_chat(body: NewChat):
    """Create a chat session (parallel or serial baseline) and return its id."""
    chat_id = uuid.uuid4().hex[:8]
    chats[chat_id] = ChatSession(
        chat_id,
        brain,
        concurrency=8 if body.mode == "parallel" else 1,
        coalesce=body.coalesce,
    )
    return {"id": chat_id, "mode": body.mode, "coalesce": body.coalesce}


@app.post("/api/chats/{chat_id}/messages")
async def post_message(chat_id: str, body: Incoming):  # async: must run on the event loop
    """Accept a chat message; returns the slot number it was given."""
    slot = get_chat(chat_id).receive(body.user, body.text, body.thread)
    return {"slot": slot}


@app.get("/api/chats/{chat_id}/state")
def state(chat_id: str):
    """Current slots and whether the chat is idle."""
    chat = get_chat(chat_id)
    return {"idle": chat.idle(), "slots": chat.snapshot()}


@app.get("/api/chats/{chat_id}/events")
async def events(chat_id: str, request: Request):
    """Server-sent event stream: the backlog, then live events."""
    chat = get_chat(chat_id)
    backlog, q = chat.subscribe()

    async def stream():
        """Yield events as SSE lines, with a keepalive every 15s."""
        try:
            for ev in backlog:
                yield f"data: {json.dumps(ev)}\n\n"
            while not await request.is_disconnected():
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"data: {json.dumps(ev)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            chat.unsubscribe(q)

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/api/massive/{path:path}")
async def massive(path: str, request: Request):
    """Debug pass-through to Massive, e.g. /api/massive/v2/aggs/ticker/AAPL/prev"""
    try:
        return await massive_get(f"/{path}", dict(request.query_params))
    except Exception as e:
        raise HTTPException(502, str(e))
