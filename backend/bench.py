"""Replay a scripted group-chat burst against real Claude + Massive, parallel vs serial.

Run: .venv/bin/python bench.py                    (all configs)
     .venv/bin/python bench.py parallel           (one config)

Configs: parallel (the design), serial (baseline: same router and workers, one worker at a time).

Reports, per message: how it was routed, and wait = arrival -> answer delivered.
"""

import asyncio
import sys
import time

from agent.brain import ClaudeBrain
from agent.session import ChatSession

# (seconds after start, user, text, thread)  thread = reply in the thread containing that slot
BURST = [
    (0.0, "alice", "jj what did NVDA close at yesterday?", None),
    (0.3, "bob", "jj how has SPY done over the last 3 months?", None),
    (0.5, "dave", "anyone else think the market's overheated?", None),  # not for jj
    (0.6, "carol", "hey jj 👋", None),
    (0.9, "dave", "jj what's the latest news on TSLA?", None),
    (1.2, "bob", "also compare it with QQQ", 1),  # thread reply, no wake word needed
    (4.0, "alice", "actually make that AMD, not NVDA", 0),
    (4.5, "carol", "@jj what does Palantir actually do, in one line?", None),
]


CONFIGS = {
    "parallel": dict(concurrency=8),
    "serial": dict(concurrency=1),
}


async def run(mode: str, brain: ClaudeBrain) -> dict:
    """Replay the burst against one config and return its timing stats."""
    chat = ChatSession(mode, brain, **CONFIGS[mode])
    start = time.monotonic()
    for at, user, text, thread in BURST:
        await asyncio.sleep(max(0, start + at - time.monotonic()))
        chat.receive(user, text, thread)
    while not chat.idle():
        await asyncio.sleep(0.1)
    total = time.monotonic() - start

    snap = chat.snapshot()
    waits = [s["delivered_at"] - s["created"] for s in snap if s["delivered_at"] is not None]
    held = [s["delivered_at"] - s["done_at"] for s in snap if s["delivered_at"] is not None]
    stats = {
        "mode": mode,
        "total": total,
        "mean_wait": sum(waits) / len(waits),
        "max_wait": max(waits),
        "mean_held": sum(held) / len(held),
    }

    print(f"\n=== {mode} — all answered in {total:.1f}s ===")
    for s in snap:
        route = s["kind"] + (f"→#{s['target']}" if s["target"] is not None else "")
        wait = f"{s['delivered_at'] - s['created']:.1f}s" if s["delivered_at"] else "—"
        print(f"#{s['n']} {s['user']:<6} {route:<12} wait={wait:<7} {s['text']}")
    for ev in chat.events:
        if ev["type"] == "answer":
            print(f"\n[{ev['t']:.1f}s  #{ev['n']} → {ev['user']}] {ev['text'][:160]}")
    return stats


async def main():
    """Run the selected configs and print a comparison table."""
    brain = ClaudeBrain()
    modes = sys.argv[1:] or list(CONFIGS)
    rows = [await run(m, brain) for m in modes]
    print("\n| config | all answered | mean wait | max wait | mean held in thread |")
    print("|---|---|---|---|---|")
    for r in rows:
        print(
            f"| {r['mode']} | {r['total']:.1f}s | {r['mean_wait']:.1f}s "
            f"| {r['max_wait']:.1f}s | {r['mean_held']:.1f}s |"
        )


if __name__ == "__main__":
    asyncio.run(main())
