"""Claude-backed router and worker."""

import asyncio
import datetime as dt
from typing import Callable

import anthropic

from .session import Triage, Update
from .tools import TOOLS, run_tool

ROUTER_MODEL = "claude-haiku-5-5"
WORKER_MODEL = "claude-haiku-5-5"
WORKER_EFFORT = "low"  # chat latency matters more than depth here
MAX_TURNS = 10

TRIAGE_SYSTEM = """You are the router for "jj", a finance assistant in a busy group chat. Several people \
send messages at once. You only see messages addressed to jj (they mention jj, reply in a thread jj is \
in, or are a quick follow-up from someone who just talked to jj, e.g. "hey jj" then "what's NVDA at?"); \
the history also shows other chat messages for context. For each new message, decide how it relates \
to the earlier messages and return:

- "new": a question or request that needs real work (market data, analysis, explanation).
- "trivial": chit-chat or something you can answer in one short line without data (greetings, thanks, \
"what can you do?"). Put the reply in `reply`.
- "edit": the message changes or adds to an earlier request: corrects a parameter, swaps a ticker, changes \
the timeframe, drops part of it, or widens it ("actually make that AMD", "I meant last month", "never mind \
the chart", "also include MSFT", "and compare with SPY"). Set `target` to the earlier message number.
- "ignore": ONLY when the new message is marked as not mentioning jj (a follow-on after someone talked \
to jj). Use it when the message is meant for other people or is general chatter ("lol bob you're wrong", \
"carol you coming tonight?"), and whenever you are unsure. A question about markets or finance phrased as \
if continuing with jj ("should I buy NVDA?", "and AMD?") is not chatter. Never use "ignore" otherwise.
- "cancel": the message withdraws an earlier request entirely ("nvm", "never mind", "forget it", "cancel \
that", "scratch that"). Set `target`. Nothing will be posted for either message. Only the asker (or someone \
clearly speaking for them) can cancel; if only part is dropped, or you're unsure, choose "edit".

Guidance:
- Edits usually come from the same person as the target, often right after it. Someone else's message is \
usually "new" unless it clearly refers to that request ("@bot for Alice's question, use weekly data").
- Prefer the most recent matching request as target. Never target a message that was itself merged; target \
the request it was merged into.
- "trivial" is only for messages that need no context at all. If answering means reading or judging what \
someone else said in the chat, choose "new" (workers see the recent transcript).
- If unsure between "new" and "edit", choose "new"; a duplicate answer is cheaper than a hijacked one.
- If the new message was posted as a reply in a thread, it is almost certainly about that thread: a change \
to that thread's pending request is "edit" targeting it; a further question is "new" (it will be \
answered inside the thread, with the thread's history).
- Keep `reason` to one short sentence.
- For "new" only, also write `brief`: ONE line for the worker who will answer it. Say who is asking and \
what they want, with every reference resolved from the chat ("it" -> SPY, "that stock" -> NVDA, "what bob \
said" -> bob's claim that NVDA will crash this week), plus any earlier answer it builds on. Don't answer the \
question and don't add facts that aren't in the chat. Omit it for other actions."""

WORKER_SYSTEM = """You are jj, a finance assistant answering one request inside a group chat. Today is {today}.
Use the market-data tools when the question needs numbers; don't guess prices.
Answer in a few short sentences or a compact list, the way you would in a chat. Address the asker by name.
A router has already read the chat; its one-line brief (when present) says what the request is about and \
resolves references, so start from it. You also get a transcript of the recent group chat for context, including messages between members that \
weren't addressed to you; use it when the request refers to what someone said. The request you are \
answering comes after the transcript.
More messages from the chat may arrive while you work, marked as changes. Fold them in: where a change \
conflicts with the request it overrides it (redo any lookups that no longer apply); otherwise it widens it. Answer the request as it stands \
at the end, not the original wording."""


def _render_history(history: list[dict]) -> str:
    """Format the recent slots as numbered lines with their status, for the router."""
    if not history:
        return "(no earlier messages)"
    rows = []
    for h in history:
        if h["kind"] == "ignored":
            state = "chat between members, not addressed to jj"
        elif h["kind"] == "merged":
            state = f"merged into #{h['target']}"
        elif h["kind"] == "pending":
            state = "still being routed"
        elif h["delivered"]:
            state = f"answered: {h['answer']!r}"
        else:
            state = f"{h['kind']}, {h['status']}"
        where = f" in thread #{h['thread']}" if h["thread"] != h["n"] else ""
        rows.append(f"#{h['n']}{where} [{h['user']}] {h['text']!r} ({state})")
    return "\n".join(rows)


def _render_updates(updates: list[Update]) -> str:
    """Format mid-run edits as a message to the worker."""
    return "\n".join(
        f"[New message while you were working] "
        f"Change from {u.user}: {u.text}"
        for u in updates
    )


def _drain(q: asyncio.Queue) -> list[Update]:
    """Take everything currently waiting in the inbox without blocking."""
    items = []
    while not q.empty():
        items.append(q.get_nowait())
    return items


class ClaudeBrain:
    def __init__(self, client: anthropic.AsyncAnthropic | None = None):
        """Wrap an Anthropic client (one is created if not given)."""
        self.client = client or anthropic.AsyncAnthropic()

    async def triage(
        self, user: str, text: str, history: list[dict], in_thread: int | None = None, soft: bool = False
    ) -> Triage:
        """Ask the router model to label one message; returns structured Triage output."""
        n = history[-1]["n"] + 1 if history else 0
        where = f", posted as a reply in thread #{in_thread}" if in_thread is not None else ""
        content = (
            f"Earlier messages (oldest first):\n{_render_history(history)}\n\n"
            f"New message #{n} from [{user}]{where}: {text!r}"
        )
        if soft:
            content += (
                f"\n\nNote: this message does not mention jj. It is only here because {user} talked to jj "
                f"moments ago. Decide whether it is meant for jj. If it is aimed at other people, is general "
                f"chatter, or you are unsure, return \"ignore\"."
            )
        resp = await self.client.messages.parse(
            model=ROUTER_MODEL,
            max_tokens=4000,
            output_config={"effort": "low"},
            system=TRIAGE_SYSTEM,
            messages=[{"role": "user", "content": content}],
            output_format=Triage,
        )
        return resp.parsed_output or Triage(action="new", reason=f"router stop_reason={resp.stop_reason}")

    async def answer(
        self, prompt: str, inbox: asyncio.Queue, progress: Callable[[str], None]
    ) -> str:
        """Worker agent loop: call the model, run tools, fold in edits each turn, return the answer."""
        system = WORKER_SYSTEM.format(today=dt.date.today().isoformat())
        messages: list[dict] = [{"role": "user", "content": prompt}]

        for _ in range(MAX_TURNS):
            progress("thinking")
            resp = await self.client.beta.messages.create(
                model=WORKER_MODEL,
                max_tokens=16000,
                output_config={"effort": WORKER_EFFORT},
                betas=["server-side-fallback-2026-07-01"],
                fallbacks="default",
                system=system,
                tools=TOOLS,
                messages=messages,
            )
            if resp.stop_reason == "refusal":
                return "I can't help with that one."
            messages.append({"role": "assistant", "content": resp.content})

            # Turn boundary: the only safe place to fold in mid-flight additions.
            extra = _drain(inbox)
            if extra:
                progress(f"folding in {len(extra)} new message(s)")

            if resp.stop_reason == "tool_use":
                calls = [b for b in resp.content if b.type == "tool_use"]
                progress(", ".join(f"{c.name}({c.input.get('ticker', '')})" for c in calls))
                results = await asyncio.gather(*(self._call(c) for c in calls))
                user_content = list(results)
                if extra:
                    user_content.append({"type": "text", "text": _render_updates(extra)})
                messages.append({"role": "user", "content": user_content})
                continue

            if extra:  # model was about to finish but the request changed under it
                messages.append({"role": "user", "content": _render_updates(extra)})
                continue

            return "".join(b.text for b in resp.content if b.type == "text").strip()

        return "Sorry, that one took too many steps; try narrowing it down."

    async def _call(self, block) -> dict:
        """Run one tool call and wrap the result (or error) as a tool_result block."""
        try:
            out = await run_tool(block.name, block.input)
            return {"type": "tool_result", "tool_use_id": block.id, "content": out}
        except Exception as e:
            return {"type": "tool_result", "tool_use_id": block.id, "content": f"error: {e}", "is_error": True}
