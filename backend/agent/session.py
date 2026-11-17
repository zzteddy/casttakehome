"""Per-chat orchestration: parallel workers, one ordered answer stream.

Model
-----
Every incoming message reserves a *slot* (0, 1, 2, ...) the moment it arrives.
Each answer is posted as soon as it is ready, as a reply in the thread of the
message it answers (like iMessage/Slack replies). Clarity comes from the reply
link, not arrival order, so one slow request never blocks another thread.
Within a thread the order is always correct: changes made before delivery merge
into the one pending answer, a follow-up only exists after the answer it
follows, and a second question's answer is held until the earlier one in its
thread is posted. Progress ("status") events are emitted immediately.

Addressing: the bot (wake word, default "jj") only acts on messages that
mention it, that are replies in a thread it is already part of, or that are
among a user's next `engage_msgs` messages within `engage_window` seconds of
them addressing jj or jj answering them. Other messages are kept as chat
context for the router but get no slot work.

A cheap router triages each message into one of:
  new      -> the slot becomes a request and gets its own worker
  trivial  -> the router's reply is the answer (no worker)
  edit     -> changes or adds to an earlier, unposted request ("actually AMD",
              "also QQQ"): merged into its prompt if not started yet, injected
              into the running worker at its next turn boundary (no restart),
              or restarted if it had finished but wasn't posted yet
  cancel   -> withdraws an earlier, not-yet-posted request ("nvm"): its worker
              is stopped and nothing is posted, for either message
  ignore   -> only for soft messages (addressed via the engagement window, no
              "jj"): the router judged it chatter meant for other people

A merged slot never emits an answer of its own. If the target was already
delivered, a change can't be applied retroactively, so the message becomes a
new request carrying the earlier Q&A as context.

Stale-answer protection: each request has a `version`. A restart bumps it and
cancels the old task; a worker only commits its answer if its version is
still current.
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from typing import Callable, Literal, Protocol

from pydantic import BaseModel


class Triage(BaseModel):
    action: Literal["new", "edit", "trivial", "cancel", "ignore"]
    target: int | None = None  # earlier slot number, for edit/cancel
    reply: str | None = None  # for trivial
    brief: str | None = None  # for new: one line handed to the worker, so it doesn't re-derive context
    reason: str = ""


@dataclass
class Update:
    kind: str  # "edit"
    user: str
    text: str
    slot: int


class Brain(Protocol):
    async def triage(
        self, user: str, text: str, history: list[dict], in_thread: int | None = None, soft: bool = False
    ) -> Triage:
        """Classify one message against the chat so far."""
        ...

    async def answer(
        self, prompt: str, inbox: asyncio.Queue, progress: Callable[[str], None]
    ) -> str:
        """Run a worker for `prompt`, folding in edits from `inbox`; return the answer text."""
        ...


@dataclass
class Slot:
    n: int
    user: str
    text: str
    created: float = field(default_factory=time.monotonic)
    kind: str = "pending"  # pending | request | trivial | merged | ignored | cancelled
    status: str = "triaging"  # triaging | queued | running | done | error | merged | cancelled
    detail: str = ""
    target: int | None = None
    version: int = 0
    answer: str | None = None
    updates: list[Update] = field(default_factory=list)
    context: str | None = None
    thread: int | None = None  # slot number of the message this thread started from
    reply_thread: int | None = None  # set when the user posted this as a thread reply
    soft: bool = False  # addressed only via the user's engagement window (no "jj")
    triage_reason: str = ""
    brief: str | None = None  # router's one-line handoff to the worker
    started_at: float | None = None
    done_at: float | None = None
    delivered_at: float | None = None
    task: asyncio.Task | None = field(default=None, repr=False)
    inbox: asyncio.Queue = field(default_factory=asyncio.Queue, repr=False)
    triaged: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    def prompt(self, transcript: str | None = None) -> str:
        """Build the worker's opening prompt: transcript, context, brief, question, edits."""
        lines = []
        if transcript:
            lines.append(transcript)
        if self.context:
            lines.append(self.context)
        if self.brief:
            lines.append(f"Router's brief (what this request is about, from the chat so far): {self.brief}")
        lines.append(f"{self.user} asked: {self.text}")
        for u in self.updates:
            lines.append(
                f"Change from {u.user} (overrides anything above it conflicts with, otherwise adds to it): {u.text}"
            )
        return "\n".join(lines)

    def public(self, t0: float) -> dict:
        """Serialize this slot for the UI, with times relative to the chat start."""
        rel = lambda t: None if t is None else round(t - t0, 2)  # noqa: E731
        return {
            "n": self.n,
            "user": self.user,
            "text": self.text,
            "kind": self.kind,
            "status": self.status,
            "detail": self.detail,
            "target": self.target,
            "thread": self.thread,
            "soft": self.soft,
            "version": self.version,
            "updates": [u.__dict__ for u in self.updates],
            "triage_reason": self.triage_reason,
            "brief": self.brief,
            "created": rel(self.created),
            "started_at": rel(self.started_at),
            "done_at": rel(self.done_at),
            "delivered_at": rel(self.delivered_at),
        }


class ChatSession:
    def __init__(
        self,
        chat_id: str,
        brain: Brain,
        concurrency: int = 8,
        wake_word: str | None = "jj",
        coalesce: float = 0.0,
        engage_window: float = 60.0,
        engage_msgs: int = 3,
    ):
        """Set up one chat's state: slots, worker limit, wake word, windows, subscribers."""
        self.id = chat_id
        # Engagement: after a user addresses jj (or jj answers them), their next
        # `engage_msgs` messages within `engage_window` seconds count as addressed.
        self.engage_window = engage_window
        self.coalesce = coalesce  # seconds of quiet before a new request's worker starts
        self.engage_msgs = engage_msgs
        self.engaged: dict[str, tuple[float, int]] = {}  # user -> (window start, soft msgs used)
        self.wake = re.compile(rf"(?<!\w)@?{re.escape(wake_word)}(?!\w)", re.I) if wake_word else None
        self.brain = brain
        self.sem = asyncio.Semaphore(concurrency)
        self.slots: list[Slot] = []
        self.events: list[dict] = []
        self.subscribers: set[asyncio.Queue] = set()
        self.t0 = time.monotonic()
        self._tasks: set[asyncio.Task] = set()

    # ---------- public API ----------

    def receive(self, user: str, text: str, thread: int | None = None) -> int:
        """Reserve a slot synchronously (arrival order == slot order), triage async.

        `thread`: slot number of any message in the thread this is a reply to.
        """
        slot = Slot(n=len(self.slots), user=user, text=text)
        if thread is not None and 0 <= thread < slot.n:
            slot.reply_thread = self.slots[thread].thread  # normalise to the thread root
        slot.thread = slot.reply_thread if slot.reply_thread is not None else slot.n
        self.slots.append(slot)
        self._publish(
            {"type": "message", "n": slot.n, "user": user, "text": text, "thread": slot.reply_thread}
        )

        if not self._addressed(slot):
            slot.kind = slot.status = "ignored"
            slot.triaged.set()
            self._emit_slot(slot)
            return slot.n

        self._emit_slot(slot)
        self._spawn(self._triage(slot))
        return slot.n

    def _addressed(self, slot: Slot) -> bool:
        """Decide whether jj should act on this message: wake word, jj's thread, or follow-on window."""
        if self.wake is None or self.wake.search(slot.text):
            self._engage(slot.user)
            return True
        if slot.reply_thread is not None and any(
            # A thread reply continues the conversation only if the bot is in that thread.
            s.thread == slot.reply_thread and s.kind != "ignored" for s in self.slots[: slot.n]
        ):
            return True
        # Follow-on without the wake word: "hey jj" ... "what's NVDA at?"
        start, used = self.engaged.get(slot.user, (None, 0))
        if start is not None and slot.created - start <= self.engage_window and used < self.engage_msgs:
            self.engaged[slot.user] = (start, used + 1)
            slot.soft = True
            return True
        return False

    def _ignore_soft(self, slot: Slot) -> None:
        """Router says this window message was chatter, not for jj. Drop it and give
        its window slot back, so side chatter doesn't use up a real follow-up."""
        slot.kind = slot.status = "ignored"
        slot.detail = "not for jj (router)"
        start, used = self.engaged.get(slot.user, (None, 0))
        if start is not None and start <= slot.created:  # same window it was counted in
            self.engaged[slot.user] = (start, max(0, used - 1))
        slot.triaged.set()
        self._emit_slot(slot)
        self._flush()  # it may have been holding up later answers in its thread

    def _engage(self, user: str) -> None:
        """Open (or restart) the user's engagement window."""
        self.engaged[user] = (time.monotonic(), 0)

    def subscribe(self) -> tuple[list[dict], asyncio.Queue]:
        """Register an event listener; return the backlog so far plus a live queue."""
        q: asyncio.Queue = asyncio.Queue()
        self.subscribers.add(q)
        return list(self.events), q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        """Stop sending events to this listener."""
        self.subscribers.discard(q)

    def idle(self) -> bool:
        """True when every slot is routed and either posted or needs no answer."""
        return all(
            s.triaged.is_set() and (s.kind in ("merged", "ignored", "cancelled") or s.delivered_at is not None)
            for s in self.slots
        )

    def snapshot(self) -> list[dict]:
        """Current state of every slot, for the UI."""
        return [s.public(self.t0) for s in self.slots]

    # ---------- triage ----------

    async def _triage(self, slot: Slot) -> None:
        """Route a slot with the router and act on its label (new, trivial, edit, cancel, ignore)."""
        history = [self._history_row(s) for s in self.slots[max(0, slot.n - 20) : slot.n]]
        try:
            t = await self.brain.triage(
                slot.user, slot.text, history, in_thread=slot.reply_thread, soft=slot.soft
            )
        except Exception as e:  # router failure must never drop a message
            t = Triage(action="new", reason=f"triage failed ({e!r}); defaulting to new")
        slot.triage_reason = t.reason
        slot.brief = (t.brief or "").strip() or None

        if t.action == "ignore":
            if slot.soft:
                self._ignore_soft(slot)
                return
            t.action = "new"  # explicitly addressed: never silently dropped

        if slot.reply_thread is not None:
            # Posted in a thread: changes default to that thread's latest request,
            # and anything new is answered in the thread with its history as context.
            in_thread = [
                s for s in self.slots[: slot.n]
                if s.thread == slot.reply_thread and s.kind in ("pending", "request", "trivial")
            ]
            if t.action in ("edit", "cancel") and in_thread and (
                t.target is None
                or not 0 <= t.target < slot.n
                or self.slots[t.target].thread != slot.reply_thread
            ):
                t.target = in_thread[-1].n
            if t.action in ("new", "trivial"):
                slot.context = self._thread_context(slot.reply_thread, slot.n)

        if t.action == "cancel":
            root = await self._root(t.target) if t.target is not None and 0 <= t.target < slot.n else None
            self._cancel(slot, root)
            return

        if t.action == "edit" and t.target is not None and 0 <= t.target < slot.n:
            root = await self._root(t.target)
            if root.kind == "request" and root.delivered_at is None:
                self._merge(slot, root, t.action)
                return
            if root.kind != "cancelled":  # an edit to a withdrawn request is just answered fresh
                # Target already answered (or was trivial): can't change history.
                # Treat as a fresh request that carries the earlier exchange as context.
                slot.context = (
                    f"This is a follow-up to an earlier message from {root.user}: {root.text!r}.\n"
                    f"The earlier answer was: {root.answer!r}"
                )
                slot.target = root.n
                slot.thread = root.thread  # reply in the same thread, after the earlier answer

        if t.action == "trivial" and t.reply:
            slot.kind, slot.status, slot.answer = "trivial", "done", t.reply
            slot.done_at = time.monotonic()
            slot.triaged.set()
            self._emit_slot(slot)
            self._flush()
            return

        slot.kind, slot.status = "request", "queued"
        slot.triaged.set()
        self._emit_slot(slot)
        self._start(slot)

    def _thread_context(self, thread: int, upto: int) -> str:
        """Earlier messages and posted answers in a thread, as worker context."""
        lines = ["Earlier in this thread:"]
        for s in self.slots[:upto]:
            if s.thread != thread or s.kind == "ignored":
                continue
            lines.append(f"{s.user}: {s.text}")
            if s.delivered_at is not None and s.answer:
                lines.append(f"you (jj): {s.answer}")
        return "\n".join(lines)

    def _transcript(self, upto: int, limit: int = 15) -> str | None:
        """Recent chat before slot `upto`, including chatter not addressed to jj,
        so a worker can answer "jj is bob right?". Built when the worker starts."""
        rows = []
        for s in self.slots[max(0, upto - limit) : upto]:
            where = f" (in thread #{s.thread})" if s.thread != s.n else ""
            rows.append(f"#{s.n}{where} {s.user}: {s.text}")
            if s.delivered_at is not None and s.answer:
                ans = s.answer if len(s.answer) <= 300 else s.answer[:300] + "…"
                rows.append(f"#{s.n} you (jj) replied: {ans}")
        if not rows:
            return None
        return "Recent group chat, for context (oldest first):\n" + "\n".join(rows) + "\n---"

    async def _root(self, n: int) -> Slot:
        """Follow merges back to the request a slot ultimately belongs to, once it's routed."""
        s = self.slots[n]
        await s.triaged.wait()
        while s.kind == "merged":  # targets are strictly earlier, so this terminates
            s = self.slots[s.target]
        return s

    def _merge(self, slot: Slot, root: Slot, action: str) -> None:
        """Apply an edit to a pending request: into its prompt, its running worker, or a restart."""
        upd = Update(kind=action, user=slot.user, text=slot.text, slot=slot.n)
        slot.kind, slot.status, slot.target = "merged", "merged", root.n
        slot.thread = root.thread
        slot.detail = f"{action} → #{root.n}"
        slot.triaged.set()
        root.updates.append(upd)

        if root.status == "queued":
            pass  # prompt is built when the worker starts; nothing else to do
        elif root.status == "running":
            root.inbox.put_nowait(upd)
            root.detail = f"folding in #{slot.n}"
        else:  # finished but not yet delivered: cheap to redo, nobody has seen it
            self._restart(root, f"restarted for {action} #{slot.n}")

        self._emit_slot(slot)
        self._emit_slot(root)
        self._flush()

    def _cancel(self, slot: Slot, root: Slot | None) -> None:
        """"nvm": withdraw `root` if it hasn't been posted yet. Neither message gets
        an answer. Nothing pending to withdraw (already posted, trivial, unknown)
        -> the cancel message is simply absorbed."""
        slot.kind, slot.status = "merged", "merged"  # never answers on its own
        slot.triaged.set()
        if root is not None and root.kind == "request" and root.delivered_at is None:
            slot.target, slot.thread = root.n, root.thread
            slot.detail = f"cancel → #{root.n}"
            if root.task and not root.task.done():
                root.task.cancel()
            root.version += 1  # any worker still finishing can't commit
            root.kind = root.status = "cancelled"
            root.answer, root.detail = None, f"cancelled by #{slot.n}"
            self._emit_slot(root)
        else:
            slot.detail = "cancel: nothing pending"
        self._emit_slot(slot)
        self._flush()  # later answers in root's thread may have been waiting on it

    # ---------- workers ----------

    def _start(self, slot: Slot) -> None:
        """Launch the worker task for a request at its current version."""
        slot.task = self._spawn(self._run(slot, slot.version))

    def _restart(self, slot: Slot, why: str) -> None:
        """Cancel any running worker, bump the version, and start again with all edits."""
        if slot.task and not slot.task.done():
            slot.task.cancel()
        slot.version += 1
        slot.inbox = asyncio.Queue()  # pending updates are already in slot.updates
        slot.answer, slot.done_at = None, None
        slot.status, slot.detail = "queued", why
        self._start(slot)

    def _related(self, s: Slot, req: Slot) -> bool:
        """Could `s` (a later message) be a continuation of request `req`?"""
        if s.kind == "merged":
            return s.target == req.n
        return s.kind == "pending" and (s.user == req.user or s.reply_thread == req.thread)

    async def _coalesce_wait(self, slot: Slot, version: int) -> None:
        """Hold a new request until its asker (or its thread) has been quiet for
        `self.coalesce` seconds, so "compare NVDA" / "and AMD" / "over 1y" start
        one worker with the whole question instead of a worker that has to fold in
        changes mid-run. Edits to a request that hasn't started are free (the
        prompt is built at start), so holding is all it takes. Also waits out
        related messages that are still being routed."""
        while version == slot.version:
            later = [s for s in self.slots[slot.n + 1 :] if self._related(s, slot)]
            routing = [s for s in later if s.kind == "pending"]
            quiet_at = max([slot.created] + [s.created for s in later]) + self.coalesce
            remaining = quiet_at - time.monotonic()
            if remaining <= 0 and not routing:
                return
            slot.detail = "waiting for more messages"
            self._emit_slot(slot)
            waits = [asyncio.ensure_future(s.triaged.wait()) for s in routing]
            if remaining > 0:
                waits.append(asyncio.ensure_future(asyncio.sleep(remaining)))
            try:
                await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for w in waits:
                    w.cancel()

    async def _run(self, slot: Slot, version: int) -> None:
        """Worker lifecycle: optional quiet wait, run under the limit, commit only if still current."""
        try:
            if self.coalesce > 0 and version == 0:  # restarts don't wait again
                await self._coalesce_wait(slot, version)
                if version != slot.version:
                    return
                slot.detail = ""
            async with self.sem:
                if version != slot.version:
                    return
                slot.status, slot.started_at = "running", time.monotonic()
                self._emit_slot(slot)
                answer = await self.brain.answer(
                    slot.prompt(self._transcript(slot.n)), slot.inbox, lambda d: self._progress(slot, version, d)
                )
            if version != slot.version:
                return
            if not slot.inbox.empty():  # an edit arrived after the worker's last check
                self._restart(slot, "restarted: addition arrived as it finished")
                return
            slot.answer, slot.status, slot.detail = answer, "done", ""
        except asyncio.CancelledError:
            return  # superseded by a restart; the new task owns the slot
        except Exception as e:
            if version != slot.version:
                return
            slot.answer, slot.status = f"Sorry, I hit an error on this one: {e}", "error"
        slot.done_at = time.monotonic()
        self._emit_slot(slot)
        self._flush()

    def _progress(self, slot: Slot, version: int, detail: str) -> None:
        """Show worker progress in the slot, ignoring updates from superseded versions."""
        if version == slot.version:
            slot.detail = detail
            self._emit_slot(slot)

    # ---------- ordered output ----------

    def _flush(self) -> None:
        """Post every finished answer, except one behind an earlier unposted request
        in the SAME thread: two questions in one thread are answered in order.
        Other threads never wait on each other."""
        blocking: dict[int, int] = {}  # thread -> earliest unposted slot in it
        for s in self.slots:
            if s.kind in ("merged", "ignored", "cancelled") or s.delivered_at is not None:
                continue
            ahead = blocking.get(s.thread)
            if ahead is None and s.status in ("done", "error"):
                self._deliver(s)
                continue
            if ahead is None:
                blocking[s.thread] = s.n  # still routing or working: later ones in its thread wait
            elif s.status in ("done", "error") and s.detail != f"ready, waiting on #{ahead} in this thread":
                s.detail = f"ready, waiting on #{ahead} in this thread"
                self._emit_slot(s)

    def _deliver(self, s: Slot) -> None:
        """Post a finished answer as a threaded reply and record when."""
        s.delivered_at = time.monotonic()
        if self.wake is not None:
            self._engage(s.user)  # jj just answered them: keep the back-and-forth open
        s.detail = ""
        root = self.slots[s.thread]
        self._publish(
            {
                "type": "answer",
                "n": s.n,
                "user": s.user,
                "text": s.answer,
                "reply_to": s.n,  # the message being answered
                "thread": s.thread,  # first message of the thread
                "quote": {"user": s.user, "text": s.text},
                "thread_root": {"user": root.user, "text": root.text},
                "merged": [u.slot for u in s.updates],
                "followup_to": s.target,
                "wait": round(s.delivered_at - s.created, 1),  # message arrival -> this reply
            }
        )
        self._emit_slot(s)

    # ---------- plumbing ----------

    def _history_row(self, s: Slot) -> dict:
        """Compact view of a slot for the router's history."""
        return {
            "n": s.n,
            "user": s.user,
            "text": s.text,
            "kind": s.kind,
            "status": s.status,
            "target": s.target,
            "thread": s.thread,
            "delivered": s.delivered_at is not None,
            "answer": (s.answer or "")[:300] or None,
        }

    def _emit_slot(self, s: Slot) -> None:
        """Publish a slot's latest state to listeners."""
        self._publish({"type": "slot", "slot": s.public(self.t0)})

    def _publish(self, ev: dict) -> None:
        """Timestamp an event, keep it in the log, and push it to every listener."""
        ev["t"] = round(time.monotonic() - self.t0, 2)
        self.events.append(ev)
        for q in self.subscribers:
            q.put_nowait(ev)

    def _spawn(self, coro) -> asyncio.Task:
        """Start a background task and keep a reference so it isn't garbage-collected."""
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task
