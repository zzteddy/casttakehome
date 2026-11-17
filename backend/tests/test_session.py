"""Deterministic tests of the orchestration layer, using a scripted fake brain.

Run: .venv/bin/python -m pytest tests -q
"""

import asyncio
import time

from agent.session import ChatSession, Triage

TICK = 0.05  # worker "turn" length in the fake


class FakeBrain:
    """Router: looks up `routes[text]`, default new.
    Worker: runs `turns` fake LLM turns (from 'slow'/'fast' in the prompt), checking the
    inbox at each turn boundary like the real worker, and echoes everything it saw."""

    def __init__(self, routes=None, triage_delay=0.0):
        self.routes = routes or {}
        self.triage_delay = triage_delay
        self.started = []  # prompts each worker run started with
        self.cancelled = 0

    async def triage(self, user, text, history, in_thread=None, soft=False):
        self.last_in_thread = in_thread
        self.last_soft = soft
        await asyncio.sleep(self.triage_delay)
        return self.routes.get(text, Triage(action="new"))

    async def answer(self, prompt, inbox, progress):
        self.started.append(prompt)
        request = prompt.split("\n---\n")[-1]  # drop the chat transcript
        turns = 20 if "slow" in request else 2
        seen = [request.replace("\n", " | ")]
        try:
            for _ in range(turns):
                await asyncio.sleep(TICK)
                while not inbox.empty():
                    seen.append(f"+{inbox.get_nowait().text}")
            return " ".join(seen)
        except asyncio.CancelledError:
            self.cancelled += 1
            raise


def mk(brain, **kw):
    """Session with the wake word off, so orchestration tests don't need 'jj' in every message."""
    kw.setdefault("wake_word", None)
    return ChatSession("c", brain, **kw)


async def run_until_idle(chat, timeout=5):
    deadline = time.monotonic() + timeout
    while not chat.idle():
        assert time.monotonic() < deadline, chat.snapshot()
        await asyncio.sleep(0.01)


def answers(chat):
    return [e for e in chat.events if e["type"] == "answer"]


def test_requests_run_concurrently():
    async def go():
        chat = mk(FakeBrain())
        chat.receive("alice", "slow question")
        chat.receive("bob", "fast question")
        chat.receive("carol", "slow question 2")
        t = time.monotonic()
        await run_until_idle(chat)
        elapsed = time.monotonic() - t
        assert sorted(a["n"] for a in answers(chat)) == [0, 1, 2]
        # ran concurrently: ~one slow job, not slow + fast + slow
        assert elapsed < (20 + 2 + 20) * TICK * 0.7

    asyncio.run(go())


def test_serial_baseline_is_slower():
    async def go(conc):
        chat = mk(FakeBrain(), concurrency=conc)
        for i in range(4):
            chat.receive("u", f"fast q{i}")
        t = time.monotonic()
        await run_until_idle(chat)
        return time.monotonic() - t

    par = asyncio.run(go(8))
    ser = asyncio.run(go(1))
    assert ser > par * 2.5


def test_correction_while_running_is_injected_without_restart():
    async def go():
        brain = FakeBrain({"actually AMD": Triage(action="edit", target=0)})
        chat = mk(brain)
        chat.receive("alice", "slow price of NVDA")
        await asyncio.sleep(TICK * 3)  # worker is mid-run
        chat.receive("alice", "actually AMD")
        await run_until_idle(chat)

        a = answers(chat)
        assert len(a) == 1 and a[0]["n"] == 0 and a[0]["merged"] == [1]
        assert "+actually AMD" in a[0]["text"]  # reached the running worker
        assert brain.cancelled == 0 and chat.slots[0].version == 0
        assert chat.slots[1].kind == "merged"

    asyncio.run(go())


def test_addition_while_running_is_injected_without_restart():
    async def go():
        brain = FakeBrain({"also MSFT": Triage(action="edit", target=0)})
        chat = mk(brain)
        chat.receive("alice", "slow compare AAPL and GOOG")
        await asyncio.sleep(TICK * 3)
        chat.receive("alice", "also MSFT")
        await run_until_idle(chat)

        a = answers(chat)
        assert len(a) == 1 and "+also MSFT" in a[0]["text"]
        assert brain.cancelled == 0 and chat.slots[0].version == 0
        assert len(brain.started) == 1

    asyncio.run(go())


def test_edit_while_queued_needs_no_restart():
    async def go():
        brain = FakeBrain({"also MSFT": Triage(action="edit", target=1)})
        chat = mk(brain, concurrency=1)
        chat.receive("bob", "slow other thing")  # holds the only worker
        chat.receive("alice", "fast AAPL")  # queued
        await asyncio.sleep(TICK)
        chat.receive("alice", "also MSFT")
        await run_until_idle(chat)

        assert chat.slots[1].version == 0 and brain.cancelled == 0
        assert "Change from alice (overrides anything above it conflicts with, otherwise adds to it): also MSFT" in by_slot(chat)[1]["text"]

    asyncio.run(go())


def test_change_to_finished_but_undelivered_answer_restarts():
    async def go():
        # alice's question is a second one in bob's thread, so its answer is held behind his
        brain = OwnQuestionBrain({"also MSFT": Triage(action="edit", target=1)})
        chat = mk(brain)
        chat.receive("bob", "slow blocker")
        chat.receive("alice", "fast AAPL", thread=0)
        await asyncio.sleep(TICK * 4)  # alice's is done, held behind bob's
        assert chat.slots[1].status == "done" and chat.slots[1].delivered_at is None
        chat.receive("alice", "also MSFT")
        await run_until_idle(chat)

        assert chat.slots[1].version == 1
        assert len(brain.started) == 3  # bob, alice, alice again after the restart
        assert "Change from alice (overrides anything above it conflicts with, otherwise adds to it): also MSFT" in chat.slots[1].prompt()
        assert [a["n"] for a in answers(chat)] == [0, 1]

    asyncio.run(go())


def by_slot(chat):
    return {a["n"]: a for a in answers(chat)}


def test_change_after_delivery_becomes_followup():
    async def go():
        brain = FakeBrain({"actually AMD": Triage(action="edit", target=0)})
        chat = mk(brain)
        chat.receive("alice", "fast NVDA")
        await run_until_idle(chat)
        chat.receive("alice", "actually AMD")
        await run_until_idle(chat)

        a = answers(chat)
        assert [x["n"] for x in a] == [0, 1]
        assert a[1]["followup_to"] == 0 and "follow-up" in a[1]["text"]

    asyncio.run(go())


def test_trivial_answered_in_order_without_worker():
    async def go():
        brain = FakeBrain({"thanks!": Triage(action="trivial", reply="anytime")})
        chat = mk(brain)
        chat.receive("alice", "slow thing")
        chat.receive("bob", "thanks!")
        await run_until_idle(chat)
        a = answers(chat)
        assert [x["n"] for x in a] == [1, 0] and a[0]["text"] == "anytime"  # not held behind alice
        assert len(brain.started) == 1

    asyncio.run(go())


def test_edit_racing_its_target_through_triage():
    """Second message's routing finishes before the first's; merge must wait for the target."""

    class SlowFirstTriage(FakeBrain):
        async def triage(self, user, text, history, in_thread=None, soft=False):
            await asyncio.sleep(TICK * 3 if text == "fast NVDA" else 0)
            return self.routes.get(text, Triage(action="new"))

    async def go():
        brain = SlowFirstTriage({"and AMD": Triage(action="edit", target=0)})
        chat = mk(brain)
        chat.receive("alice", "fast NVDA")
        chat.receive("alice", "and AMD")
        await run_until_idle(chat)
        a = answers(chat)
        assert len(a) == 1 and "and AMD" in a[0]["text"]

    asyncio.run(go())


def test_router_failure_falls_back_to_new():
    class Broken(FakeBrain):
        async def triage(self, *a, **kw):
            raise RuntimeError("router down")

    async def go():
        chat = mk(Broken())
        chat.receive("alice", "fast q")
        await run_until_idle(chat)
        assert len(answers(chat)) == 1

    asyncio.run(go())


def test_thread_mode_fast_answers_do_not_wait():
    async def go():
        chat = mk(FakeBrain())  # thread ordering is the default
        chat.receive("alice", "slow question")
        chat.receive("bob", "fast question")
        await run_until_idle(chat)
        a = answers(chat)
        assert [x["n"] for x in a] == [1, 0]  # bob's reply posts first
        assert a[0]["reply_to"] == 1 and a[0]["quote"]["text"] == "fast question"
        bob = chat.slots[1]
        assert bob.delivered_at - bob.done_at < 0.01  # not held

    asyncio.run(go())


def test_thread_mode_followup_lands_in_same_thread_after_answer():
    async def go():
        brain = FakeBrain({"actually AMD": Triage(action="edit", target=0)})
        chat = mk(brain)
        chat.receive("alice", "fast NVDA")
        await run_until_idle(chat)
        chat.receive("alice", "actually AMD")
        await run_until_idle(chat)
        a = answers(chat)
        assert [x["n"] for x in a] == [0, 1]
        assert a[1]["thread"] == 0 and a[1]["followup_to"] == 0

    asyncio.run(go())


def test_thread_mode_merge_keeps_one_answer_per_thread():
    async def go():
        brain = FakeBrain({"also MSFT": Triage(action="edit", target=0)})
        chat = mk(brain)
        chat.receive("alice", "slow AAPL")
        chat.receive("bob", "fast other")
        await asyncio.sleep(TICK * 3)
        chat.receive("alice", "also MSFT")
        await run_until_idle(chat)
        threads = [x["thread"] for x in answers(chat)]
        assert sorted(threads) == [0, 1]  # one reply per thread, merged slot 2 has none
        assert chat.slots[2].thread == 0

    asyncio.run(go())


def test_only_messages_addressed_to_jj_get_answers():
    async def go():
        brain = FakeBrain()
        chat = mk(brain, wake_word="jj")
        chat.receive("alice", "anyone watching NVDA today?")  # members chatting
        chat.receive("bob", "jj, fast what did NVDA close at?")
        chat.receive("carol", "@JJ fast and AMD?")
        chat.receive("dave", "my jjj key is stuck")  # not a whole-word mention
        await run_until_idle(chat)
        assert {a["n"] for a in answers(chat)} == {1, 2}
        assert chat.slots[0].kind == chat.slots[3].kind == "ignored"
        assert len(brain.started) == 2

    asyncio.run(go())


def test_thread_reply_without_wake_word_continues_conversation():
    async def go():
        brain = FakeBrain()
        chat = mk(brain, wake_word="jj")
        chat.receive("alice", "jj fast NVDA close?")
        await run_until_idle(chat)
        chat.receive("bob", "fast how about vs last week?", thread=0)  # no "jj"
        await run_until_idle(chat)
        a = answers(chat)
        assert [x["n"] for x in a] == [0, 1] and a[1]["thread"] == 0
        assert brain.last_in_thread == 0
        assert "Earlier in this thread" in a[1]["text"] and "you (jj)" in a[1]["text"]

    asyncio.run(go())


def test_thread_reply_to_ignored_chatter_is_ignored():
    async def go():
        chat = mk(FakeBrain(), wake_word="jj")
        chat.receive("alice", "lunch?")
        chat.receive("bob", "sure", thread=0)
        await run_until_idle(chat)
        assert answers(chat) == [] and chat.slots[1].kind == "ignored"

    asyncio.run(go())


def test_thread_reply_while_pending_merges_into_that_thread():
    async def go():
        # router says "edit" but names no target; the thread decides it
        brain = FakeBrain({"also MSFT": Triage(action="edit")})
        chat = mk(brain, wake_word="jj")
        chat.receive("alice", "jj slow AAPL")
        chat.receive("bob", "jj fast other")
        await asyncio.sleep(TICK * 3)
        chat.receive("alice", "also MSFT", thread=0)
        await run_until_idle(chat)
        assert chat.slots[2].kind == "merged" and chat.slots[2].target == 0
        assert len([a for a in answers(chat) if a["thread"] == 0]) == 1

    asyncio.run(go())


def test_worker_sees_recent_chat_including_chatter():
    async def go():
        brain = FakeBrain()
        chat = mk(brain, wake_word="jj")
        chat.receive("bob", "NVDA will double by friday")  # not for jj
        chat.receive("alice", "jj fast is bob right?")
        await run_until_idle(chat)
        assert "bob: NVDA will double by friday" in brain.started[0]
        assert brain.started[0].rstrip().endswith("alice asked: jj fast is bob right?")

    asyncio.run(go())


# ---------- coalescing wait ----------

def test_coalesce_split_messages_start_one_worker():
    async def go():
        brain = FakeBrain({
            "and AMD": Triage(action="edit", target=0),
            "over 1y": Triage(action="edit", target=0),
        })
        chat = mk(brain, coalesce=TICK * 4)
        chat.receive("bob", "compare NVDA")
        await asyncio.sleep(TICK * 2)
        chat.receive("bob", "and AMD")
        await asyncio.sleep(TICK * 2)  # still inside the window (it reset on "and AMD")
        chat.receive("bob", "over 1y")
        await run_until_idle(chat)

        a = answers(chat)
        assert len(a) == 1 and a[0]["merged"] == [1, 2]
        assert len(brain.started) == 1  # one worker run, all three messages in its prompt
        assert "and AMD" in brain.started[0] and "over 1y" in brain.started[0]
        assert brain.cancelled == 0 and chat.slots[0].version == 0

    asyncio.run(go())


def test_coalesce_waits_for_related_message_still_routing():
    async def go():
        brain = FakeBrain({"and AMD": Triage(action="edit", target=0)}, triage_delay=TICK * 3)
        chat = mk(brain, coalesce=TICK)
        chat.receive("bob", "compare NVDA")
        await asyncio.sleep(TICK * 0.5)
        chat.receive("bob", "and AMD")  # routing outlasts the window
        await run_until_idle(chat)

        assert len(brain.started) == 1 and "and AMD" in brain.started[0]

    asyncio.run(go())


def test_coalesce_ignores_other_users():
    async def go():
        chat = mk(FakeBrain(), coalesce=TICK * 2)
        chat.receive("bob", "fast NVDA")
        for i in range(4):  # carol keeps talking; bob's request shouldn't wait on her
            await asyncio.sleep(TICK)
            chat.receive("carol", f"fast q{i}")
        await run_until_idle(chat)
        bob = chat.slots[0]
        assert bob.started_at - bob.created < TICK * 3

    asyncio.run(go())


def test_cancel_during_coalescing_wait_never_starts_worker():
    async def go():
        brain = FakeBrain({"nvm": Triage(action="cancel", target=0)})
        chat = mk(brain, coalesce=TICK * 4)
        chat.receive("alice", "fast price of NVDA")
        await asyncio.sleep(TICK)
        chat.receive("alice", "nvm")
        await run_until_idle(chat)
        await asyncio.sleep(TICK * 6)
        assert brain.started == [] and answers(chat) == []

    asyncio.run(go())


# ---------- engagement window ("hey jj" then a follow-up without jj) ----------

def test_engagement_followup_without_wake_word_is_answered():
    async def go():
        brain = FakeBrain({"hey jj": Triage(action="trivial", reply="hey!")})
        chat = ChatSession("c", brain, engage_window=TICK * 100)
        chat.receive("alice", "hey jj")
        chat.receive("alice", "fast what's NVDA at?")
        await run_until_idle(chat)
        assert [a["n"] for a in answers(chat)] == [0, 1]
        assert chat.slots[1].soft and chat.slots[1].kind == "request"

    asyncio.run(go())


def test_engagement_caps_at_n_messages_and_is_per_user():
    async def go():
        brain = FakeBrain()
        chat = ChatSession("c", brain, engage_window=TICK * 100, engage_msgs=3)
        chat.receive("alice", "jj slow first")  # long-running, so no answer resets the window
        for i in range(4):
            chat.receive("alice", f"slow more {i}")
        chat.receive("bob", "slow unrelated")  # alice's window doesn't cover bob
        kinds = [s.kind for s in chat.slots]
        assert [s.soft for s in chat.slots[1:4]] == [True] * 3
        assert kinds[4] == "ignored"  # 4th un-addressed message: cap reached
        assert kinds[5] == "ignored"  # bob
        await run_until_idle(chat)

    asyncio.run(go())


def test_engagement_window_expires():
    async def go():
        chat = ChatSession("c", FakeBrain(), engage_window=TICK)
        chat.receive("alice", "jj slow q")
        await asyncio.sleep(TICK * 2)
        chat.receive("alice", "fast later q")
        assert chat.slots[1].kind == "ignored"
        await run_until_idle(chat)

    asyncio.run(go())


def test_engagement_answer_restarts_window():
    async def go():
        chat = ChatSession("c", FakeBrain(), engage_window=TICK * 100, engage_msgs=1)
        chat.receive("alice", "jj fast q")
        chat.receive("alice", "fast follow-up")  # uses the only soft message
        await run_until_idle(chat)  # jj answers -> window and count reset
        chat.receive("alice", "fast another")
        assert chat.slots[2].soft
        await run_until_idle(chat)

    asyncio.run(go())


# ---------- cancel ("nvm") ----------

def test_cancel_while_running_stops_worker_and_posts_nothing():
    async def go():
        brain = FakeBrain({"nvm": Triage(action="cancel", target=0)})
        chat = mk(brain)
        chat.receive("alice", "slow price of NVDA")
        await asyncio.sleep(TICK * 3)  # worker is mid-run
        chat.receive("alice", "nvm")
        await run_until_idle(chat)
        await asyncio.sleep(TICK * 25)  # long enough for the old worker to have finished
        assert answers(chat) == []
        assert brain.cancelled == 1
        assert chat.slots[0].kind == "cancelled" and chat.slots[1].kind == "merged"

    asyncio.run(go())


def test_cancel_while_queued_never_starts_worker():
    async def go():
        brain = FakeBrain({"nvm": Triage(action="cancel", target=1)})
        chat = mk(brain, concurrency=1)
        chat.receive("bob", "slow blocker")  # holds the only worker
        chat.receive("alice", "fast price of NVDA")  # queued behind it
        await asyncio.sleep(TICK)
        chat.receive("alice", "nvm")
        await run_until_idle(chat)
        assert len(brain.started) == 1  # only bob's
        assert [a["n"] for a in answers(chat)] == [0]

    asyncio.run(go())


def test_cancel_after_delivery_is_absorbed():
    async def go():
        brain = FakeBrain({"nvm": Triage(action="cancel", target=0)})
        chat = mk(brain)
        chat.receive("alice", "fast price of NVDA")
        await run_until_idle(chat)
        chat.receive("alice", "nvm")
        await run_until_idle(chat)
        assert [a["n"] for a in answers(chat)] == [0]  # the posted answer stays; nvm adds nothing
        assert chat.slots[0].kind == "request" and chat.slots[1].detail == "cancel: nothing pending"

    asyncio.run(go())


def test_thread_reply_nvm_without_target_cancels_thread_request():
    async def go():
        brain = FakeBrain({"nvm": Triage(action="cancel")})  # router gave no target
        chat = mk(brain)
        chat.receive("alice", "slow NVDA")
        chat.receive("bob", "slow SPY")
        await asyncio.sleep(TICK)
        chat.receive("alice", "nvm", thread=0)
        await run_until_idle(chat)
        assert [a["n"] for a in answers(chat)] == [1]
        assert chat.slots[0].kind == "cancelled"

    asyncio.run(go())


# ---------- router check on engagement-window messages ----------

def test_window_chatter_ignored_by_router():
    async def go():
        brain = FakeBrain({
            "hey jj": Triage(action="trivial", reply="hey!"),
            "lol bob you're wrong": Triage(action="ignore"),
        })
        chat = ChatSession("c", brain, engage_window=TICK * 100)
        chat.receive("alice", "hey jj")
        chat.receive("alice", "lol bob you're wrong")
        await run_until_idle(chat)
        assert brain.last_soft is True  # router was told it's a window message
        assert [a["n"] for a in answers(chat)] == [0]
        assert chat.slots[1].kind == "ignored"

    asyncio.run(go())


def test_ignored_chatter_gives_window_slot_back():
    async def go():
        brain = FakeBrain({f"chatter {i}": Triage(action="ignore") for i in range(3)})
        chat = ChatSession("c", brain, engage_window=TICK * 100, engage_msgs=1)
        chat.receive("alice", "jj slow first")  # long-running: no answer resets the window
        for i in range(3):
            chat.receive("alice", f"chatter {i}")
            await asyncio.sleep(TICK / 2)  # let the router drop it before the next one
        chat.receive("alice", "slow real follow-up")
        assert chat.slots[4].soft  # still within the 1-message cap: chatter didn't use it up
        await run_until_idle(chat)
        assert sorted(a["n"] for a in answers(chat)) == [0, 4]

    asyncio.run(go())


def test_ignore_on_explicit_mention_is_treated_as_new():
    async def go():
        brain = FakeBrain({"jj fast hmm": Triage(action="ignore")})
        chat = ChatSession("c", brain)
        chat.receive("alice", "jj fast hmm")
        await run_until_idle(chat)
        assert brain.last_soft is False
        assert [a["n"] for a in answers(chat)] == [0]

    asyncio.run(go())


# ---------- order within a thread ----------

class OwnQuestionBrain(FakeBrain):
    """Speed comes from the question's own line only, not from thread context
    (a thread reply's prompt quotes the root, which may say "slow")."""

    async def answer(self, prompt, inbox, progress):
        own = [l for l in prompt.splitlines() if " asked: " in l][-1]
        return await super().answer(own, inbox, progress)


def test_thread_second_question_waits_for_first_in_same_thread():
    async def go():
        chat = mk(OwnQuestionBrain())
        chat.receive("alice", "slow NVDA deep dive")
        await asyncio.sleep(TICK)
        chat.receive("alice", "fast what's the ticker for Nvidia?", thread=0)  # new q, same thread
        chat.receive("bob", "fast SPY")  # another thread: must not wait
        await run_until_idle(chat)
        assert [a["n"] for a in answers(chat)] == [2, 0, 1]
        held = chat.slots[1]
        assert held.done_at < chat.slots[0].delivered_at <= held.delivered_at
        bob = chat.slots[2]
        assert bob.delivered_at - bob.done_at < 0.01

    asyncio.run(go())


def test_thread_hold_released_when_earlier_request_cancelled():
    async def go():
        brain = FakeBrain({"nvm": Triage(action="cancel", target=0)})
        chat = mk(brain)
        chat.receive("alice", "slow NVDA deep dive")
        chat.receive("alice", "fast other q", thread=0)
        await asyncio.sleep(TICK * 4)
        assert answers(chat) == []  # held behind #0
        chat.receive("alice", "nvm", thread=0)
        await run_until_idle(chat)
        assert [a["n"] for a in answers(chat)] == [1]

    asyncio.run(go())


def test_thread_earlier_message_still_routing_holds_later_answer():
    async def go():
        # #1 is slow to route; #2 (same thread) routes and finishes first but must wait for #1
        class Brain(OwnQuestionBrain):
            async def triage(self, user, text, history, in_thread=None, soft=False):
                if text == "fast and AMD?":
                    await asyncio.sleep(TICK * 4)
                return Triage(action="new")
        chat = mk(Brain())
        chat.receive("alice", "fast NVDA")
        chat.receive("alice", "fast and AMD?", thread=0)
        chat.receive("alice", "fast and INTC?", thread=0)
        await run_until_idle(chat)
        assert [a["n"] for a in answers(chat)] == [0, 1, 2]
        assert chat.slots[2].done_at < chat.slots[1].delivered_at  # was ready, held

    asyncio.run(go())


# ---------- router brief ----------

def test_router_brief_reaches_worker_prompt():
    async def go():
        brain = FakeBrain({"fast how has it done?": Triage(action="new", brief="bob wants SPY's 3-month performance")})
        chat = mk(brain)
        chat.receive("bob", "fast how has it done?")
        await run_until_idle(chat)
        p = brain.started[0]
        assert "Router's brief" in p and "SPY's 3-month performance" in p
        assert p.index("Router's brief") < p.index("bob asked:")
        assert chat.snapshot()[0]["brief"] == "bob wants SPY's 3-month performance"

    asyncio.run(go())
