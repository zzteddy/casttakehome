A group-chat finance agent that works on many requests at once and still gives
the chat one clear, ordered stream of answers. It also handles people editing
or adding to a request while it is still running.

## Design

```
message ──► reserve slot #n (sync, arrival order)
              │
              ▼
         router (Haiku 5.5, structured output)
     ┌────────┼──────────────────┬──────────────────┐
   trivial   new                edit               cancel ("nvm")
     │        │                  │                  │
  reply    worker            target running?      stop the worker,
     │   (Haiku 5.5 +        → push into its      post nothing
     │    Massive tools)       inbox, folded in
     │        │                at next turn
     │        │                  │
     └────────┴──► delivery: reply in the message's thread as soon as ready
                   (held only behind an earlier unposted request in that thread)
```

It combines Castle's two suggested approaches. A small router model triages
every message, the router itself answers the simple ones, and every request
that needs real work gets its own worker (a subagent with tools) running in
parallel.

**Key decisions**

- **The bot answers only when it's addressed.** It acts on a message that
  mentions **jj** ("jj …" or "@jj"), or on a reply in a thread it's already
  part of, which needs no wake word. Other chatter is shown to the router as
  context but never gets a worker. A thread reply defaults to that thread: if
  the thread's request is still pending, the reply merges into it; otherwise
  it's answered in the thread, with the thread's history as context.
  Follow-ons count too: after someone addresses jj, their next 3 messages
  within 60s are treated as addressed ("hey jj" … "what's NVDA at?"). jj
  answering them, or them saying "jj" again, restarts the window. The window
  is per user. Because these messages don't mention jj, the router is told so
  and may label them `ignore` (chatter aimed at other people), and is told to
  ignore them when unsure. An ignored message doesn't count toward the 3. A
  message that does mention jj is never ignored: if the router says `ignore`,
  it's treated as new.

- **A slot is reserved when a message arrives, not when it has been triaged.**
  Routing runs in parallel too, so a fast-to-route message can't jump ahead of
  an earlier one. If an edit's routing finishes before its target's routing,
  it waits for the target.
- **Order comes from reply threads, not from arrival order.** The task asks
  for "a clear, ordered stream of answers". The point of that is knowing which
  answer belongs to which question. Each answer is posted as soon as it is
  ready, as a reply quoting the message it answers (as in iMessage or Slack),
  so a slow request never blocks anyone else's. Within a thread the order is
  guaranteed: changes made before delivery merge into the one pending answer,
  a follow-up after delivery is a later reply in the same thread, and a second
  question asked in a thread while the first is still unanswered has its
  answer held until the first is posted. Other threads never wait.
- **The router hands the worker a one-line brief.** For a new request, the
  router writes one line in the same call, saying who's asking and what they
  want with references resolved ("that" → SPY's 3-month result, "is dave
  right?" → dave's claim NVDA will crash). The worker starts from it instead
  of working it out again from the transcript.
- **Every change to a request is one kind of edit.** A correction ("actually
  AMD") and an addition ("also QQQ") are handled the same way, and the worker
  model decides what the change means: a correction overrides what it
  conflicts with, an addition widens the request. That removes the router's
  hardest call (correction vs. addition) and keeps work in progress instead
  of restarting.
  - If the target is still queued, the change is simply added to its prompt.
  - If the target is running, it is injected into the worker's conversation
    at the next turn boundary, so no work is thrown away.
  - If the target has finished but its answer is still held behind an earlier
    question in its thread, the worker restarts. The answer hasn't been seen,
    so it is still cheap to fix.
  - _Cancel_ withdraws the whole request ("nvm", "forget it"). Its worker is
    stopped and nothing is posted for either message. If the answer has
    already been posted, the "nvm" is absorbed silently. Dropping only part
    of a request ("never mind the chart") stays an edit.
- **A new request waits for 3s of quiet before its worker starts.** People
  often split one thought across messages ("jj compare NVDA" / "and AMD" /
  "over the last year"). The worker waits until the asker, or anyone
  replying in its thread, has sent nothing related for `coalesce` seconds
  (3s in the app, 0 in tests and the benchmark). It also waits for any
  related message still being routed. Edits made during the wait are free,
  because the prompt is built when the worker starts. Measured on that
  example: one worker run with the whole question, answered at 9.3s, versus
  9.8s and an extra model call without the wait. Other users' messages
  don't extend the wait, and restarts don't wait again. Cost: up to 3s
  added to every new request, including single-message ones.
- **Once an answer is delivered it can't be changed.** A later change becomes
  a new request that carries the earlier Q&A as context ("follow-up to #0").
- **Version numbers stop stale answers.** Every restart increments the
  request's version, and a worker only commits its answer if its version is
  still current. If an addition arrives in the gap after the worker's last
  inbox check, the request restarts instead of losing the addition.
- **If unsure, the router treats a message as new, and if routing fails the
  message becomes new.** A duplicate answer is cheaper than one person's
  message being merged into someone else's request.
- **One merged answer per request.** A merged message never produces an answer
  of its own; the target's answer lists the slots it folded in.

**Trade-off of threaded replies.** The main channel is less tidy, because
answers arrive in the order they finish. The design also depends on the chat
platform supporting replies.

## Results

`bench.py` replays a seven-message burst from four users against real Claude
and Massive. The burst includes chit-chat, an addition from Bob and a
correction from Alice. It compares parallel workers against a serial baseline: the same
router and workers, one worker at a time. The router and workers both run
on Haiku 5.5, the coalescing wait is off, and each configuration was run
once.

| config            | all answered | mean wait | max wait |
| ----------------- | ------------ | --------- | -------- |
| parallel          | 7.2s         | 3.0s      | 4.7s     |
| serial (baseline) | 10.7s        | 4.3s      | 7.9s     |

_Wait_ runs from a message's arrival to its answer being delivered.
Parallelism cuts mean wait about 1.4× and max wait about 1.7×. With fast
Haiku workers, a 7-message burst barely queues even when run serially. The
gap grows with the size of the burst and the length of each request.
(With Sonnet workers, the same burst gave 4.4s vs 8.7s.)

`tests/test_session.py` holds 37 tests of the orchestration logic, using a
scripted fake brain. They need no API calls and cover:

- requests running concurrently, and parallel vs serial speed
- a correction and an addition while running (injected, no restart)
- an edit while queued
- a change to an answer that is finished but not yet delivered
- a change after delivery (becomes a follow-up)
- trivial replies posted at once, with no worker and not held behind others
- threads: fast answers aren't held, follow-ups land in the same thread, one
  answer per thread
- the jj rule: only addressed messages get answers, a thread reply works
  without the wake word, and a reply to ignored chatter is ignored
- an edit whose routing finishes before its target's
- the router failing
- the follow-on window: 3 messages / 60s, per user, restarted by an answer,
  and the router dropping chatter in it
- coalescing: split messages start one worker, it waits for related
  routing, other users don't extend it
- cancel: while running, while queued, during the wait, after posting
- order within a thread: a second question's answer waits for the first
- the router's brief reaching the worker

## Run

```
# backend (port 8001)
cd backend
uv venv .venv && uv pip install -p .venv -r requirements.txt
.venv/bin/uvicorn main:app --reload --port 8001 --timeout-graceful-shutdown 1

# frontend (port 5173, proxies /api)
cd frontend && npm install && npm run dev

# tests / benchmark
cd backend
.venv/bin/python -m pytest tests -q
.venv/bin/python bench.py            # costs a few API calls
```

The backend needs `backend/.env` containing `ANTHROPIC_API_KEY` and
`MASSIVE_API_KEY`.

In the UI, click **Run burst demo**. The chat on the left shows answers
arriving as threaded replies. The panel on the right shows each slot's route,
status, restarts (v1, v2…), the router's reason and brief, and whether an
answer is waiting on an earlier one in its thread. Click **reply**, a thread
tag or "N replies" to open that thread in the right panel, with its own input
(no "jj" needed there). Use the selector to switch between parallel and
serial.

## Layout

- `backend/agent/session.py` covers slots, triage handling, merging,
  restarts and the ordered sequencer. All the concurrency logic is here.
- `backend/agent/brain.py` holds the Claude router and the worker's agent
  loop, including where updates are injected at each turn boundary.
- `backend/agent/tools.py` defines the Massive tools: previous close, price
  history, ticker details and news.
- `backend/main.py` is the FastAPI app: create a chat, post a message, get the
  SSE event stream, get the current state.
- `frontend/src/App.jsx` is the group-chat UI, the thread pane and the slot
  panel.

## Known limitations

This is a foundation, not a production service. What I know is missing,
most serious first:

**Could cause a wrong or missing answer**

- **No time limit per request.** Model calls use the SDK defaults (10-minute
  timeout, 2 retries) and there's no overall cap. A hung request also holds
  every later answer in its thread.
- **Router mistakes aren't recoverable,** and their rate is unmeasured. If a
  follow-on is labelled `new`, or an edit is attached to the wrong request,
  nothing corrects it. There's no eval set for the router beyond the
  benchmark burst.
- **Only the asker should edit or cancel, but that's a prompt instruction, not
  enforced in code.** Bob saying "nvm" could cancel Alice's request.
- **Duplicate messages get duplicate answers.** The same question sent twice
  gets two replies, which can post in reverse order.
- **Mid-run corrections keep stale context.** After "actually AMD", the
  worker still has its NVDA steps in context and is trusted to set them aside.
- **A trivial answer held behind an earlier question in its thread can't be
  edited.** The edit becomes a follow-up, so both get posted. Rare.
- **The quiet wait has no upper limit.** Each related message (from the asker,
  or anyone replying in the request's thread) restarts the 3s wait. If people
  keep talking in that thread at least every 3s, the worker never starts and
  the request is never answered. A maximum, such as starting regardless 10s
  after the first message, would fix it.

**Under load**

- **Router calls have no concurrency cap.** Only workers are capped (8 per
  chat). A large burst could hit API rate limits. Each failed route falls
  back to `new`, which then starts unneeded workers.
- **Edits to a running worker are unbounded.** Heavy editing ends in the
  worker's 10-step limit and a "try narrowing it down" reply.
- **Memory only grows.** Chats are never removed, and each keeps every event.
- **Fixed context windows:** the router sees the last 20 messages, the worker
  the last 15. Older context in a busy chat is lost unless it's in the thread.

**Trust and safety**

- **No authentication.** The `user` field is whatever the client sends.
- **Prompt injection.** Everyone's chat messages go into the prompts. The
  damage is limited because the tools are read-only market data, but a
  message could still steer an answer.
- **No financial-advice guardrails.** "should I buy NVDA" gets a straight
  answer.

**Not built**

- Persistence and multiple processes. Everything is in memory, one event loop
  per chat. Scaling means sharding chats across processes.
- Prompt caching. Each worker's transcript window slides, so workers share no
  cached prefix beyond the system prompt and tools.
