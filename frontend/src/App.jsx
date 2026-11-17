import { useEffect, useRef, useState } from "react";
import "./styles.css";

const USERS = ["alice", "bob", "carol", "dave"];

// Minimal inline markdown for bot answers: **bold** and `code`. Line breaks and
// "- " lists already read fine thanks to white-space: pre-wrap on .msg.
// Builds React elements (no innerHTML), so model output can't inject markup.
function Md({ text }) {
  if (!text) return null;
  return text.split(/(\*\*[^*\n]+\*\*|`[^`\n]+`)/g).map((part, i) => {
    if (part.startsWith("**") && part.endsWith("**") && part.length > 4)
      return <strong key={i}>{part.slice(2, -2)}</strong>;
    if (part.startsWith("`") && part.endsWith("`") && part.length > 2)
      return <code key={i}>{part.slice(1, -1)}</code>;
    return part;
  });
}

// Seconds of quiet before a new request's worker starts; edits before then are free.
const WAITS = [0, 1, 3, 5, 10];

const CONFIGS = {
  parallel: { mode: "parallel", label: "parallel" },
  serial: { mode: "serial", label: "serial (baseline)" },
};

// [seconds, user, text, thread]: thread = reply in the thread containing that slot
const BURST = [
  [0.0, "alice", "jj what did NVDA close at yesterday?", null],
  [0.3, "bob", "jj how has SPY done over the last 3 months?", null],
  [0.5, "dave", "anyone else think the market's overheated?", null],
  [0.6, "carol", "hey jj 👋", null],
  [0.9, "dave", "jj what's the latest news on TSLA?", null],
  [1.2, "bob", "also compare it with QQQ", 1],
  [4.0, "alice", "actually make that AMD, not NVDA", 0],
  [4.5, "carol", "@jj what does Palantir actually do, in one line?", null],
];

async function post(url, body) {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return r.json();
}

const color = (u) => `user-${USERS.indexOf(u) % 4}`;

export default function App() {
  const [config, setConfig] = useState("parallel");
  const [wait, setWait] = useState(3);
  const [chatId, setChatId] = useState(null);
  const [feed, setFeed] = useState([]); // message + answer events, in emit order
  const [slots, setSlots] = useState({}); // n -> latest slot snapshot
  const [user, setUser] = useState("alice");
  const [text, setText] = useState("");
  const [openThread, setOpenThread] = useState(null); // root slot n of the thread shown in the side pane
  const [threadText, setThreadText] = useState("");
  const feedEnd = useRef(null);
  const threadEnd = useRef(null);
  const threadInput = useRef(null);

  async function newChat(c = config, w = wait) {
    const { mode } = CONFIGS[c];
    const { id } = await post("/api/chats", { mode, coalesce: w });
    setFeed([]);
    setSlots({});
    setOpenThread(null);
    setChatId(id);
  }

  useEffect(() => {
    newChat();
  }, []);

  useEffect(() => {
    if (!chatId) return;
    const es = new EventSource(`/api/chats/${chatId}/events`);
    es.onmessage = (e) => {
      const ev = JSON.parse(e.data);
      if (ev.type === "slot") setSlots((s) => ({ ...s, [ev.slot.n]: ev.slot }));
      else setFeed((f) => [...f, ev]);
    };
    // Chats live in backend memory: after a backend restart this chat id is gone (404),
    // the browser gives up on the stream, and we start a fresh chat.
    es.onerror = () => {
      if (es.readyState === EventSource.CLOSED) newChat();
    };
    return () => es.close();
  }, [chatId]);

  useEffect(() => {
    feedEnd.current?.scrollIntoView({ behavior: "smooth" });
    threadEnd.current?.scrollIntoView({ behavior: "smooth" });
  }, [feed, openThread]);

  useEffect(() => {
    if (openThread != null) threadInput.current?.focus();
  }, [openThread]);

  function send(u, t, thread = null) {
    if (!t.trim() || !chatId) return;
    post(`/api/chats/${chatId}/messages`, { user: u, text: t, thread }).then((r) => {
      if (r.detail === "unknown chat") newChat(); // backend restarted; message was not delivered
    });
  }

  function burst() {
    BURST.forEach(([at, u, t, th]) => setTimeout(() => send(u, t, th), at * 1000));
  }

  // Which thread an event belongs to. A message's slot can move into a thread after
  // routing (merged as an edit, or turned into a follow-up), so prefer the slot's view.
  const threadOf = (ev) =>
    ev.type === "answer" ? ev.thread : slots[ev.n]?.thread ?? ev.thread ?? ev.n;
  const replyCount = {};
  for (const ev of feed) {
    const th = threadOf(ev);
    if (!(ev.type === "message" && ev.n === th)) replyCount[th] = (replyCount[th] || 0) + 1;
  }
  const threadFeed = openThread == null ? [] : feed.filter((ev) => threadOf(ev) === openThread);

  const ThreadLink = ({ thread, children }) => (
    <button className="reply" onClick={() => setOpenThread(thread)}>
      {children}
    </button>
  );

  function renderEvent(ev, i, inPane = false) {
    if (ev.type === "message") {
      const th = threadOf(ev);
      const isRoot = th === ev.n;
      return (
        <div key={i} className={`msg ${color(ev.user)} ${slots[ev.n]?.kind === "ignored" ? "ignored" : ""}`}>
          <span className="who">
            #{ev.n} {ev.user}
            {!isRoot && !inPane && (
              <>
                {" · "}
                <ThreadLink thread={th}>↳ thread #{th}</ThreadLink>
              </>
            )}
            {slots[ev.n]?.kind === "ignored" && " · not for jj"}
            {!inPane && <ThreadLink thread={th}>reply</ThreadLink>}
          </span>
          {ev.text}
          {isRoot && !inPane && replyCount[th] > 0 && (
            <ThreadLink thread={th}>
              {replyCount[th]} {replyCount[th] === 1 ? "reply" : "replies"} ›
            </ThreadLink>
          )}
        </div>
      );
    }
    return (
      <div key={i} className="msg bot">
        <div className={`quote ${color(ev.quote.user)}`}>
          <span className="who">
            ↩ #{ev.reply_to} {ev.quote.user}
            {ev.thread !== ev.reply_to && !inPane && ` · thread #${ev.thread}`}
          </span>
          {ev.quote.text}
          {ev.merged?.length > 0 && <span className="who">+ folded in #{ev.merged.join(", #")}</span>}
        </div>
        <span className="who">
          jj → @{ev.user}
          <span className="time" title={`delivered ${ev.t}s into the chat`}>
            {ev.wait != null ? `replied in ${ev.wait}s` : `${ev.t}s`}
          </span>
          {!inPane && <ThreadLink thread={ev.thread}>reply</ThreadLink>}
        </span>
        <div className="body">
          <Md text={ev.text} />
        </div>
      </div>
    );
  }

  const slotList = Object.values(slots).sort((a, b) => a.n - b.n);

  return (
    <div className="app">
      <header>
        <h1>Group chat agent</h1>
        <div className="controls">
          <select
            value={config}
            onChange={(e) => {
              setConfig(e.target.value);
              newChat(e.target.value);
            }}
          >
            {Object.entries(CONFIGS).map(([k, v]) => (
              <option key={k} value={k}>
                {v.label}
              </option>
            ))}
          </select>
          <select
            value={wait}
            title="Seconds of quiet before a request starts work. Edits before then are free; changing this starts a new chat."
            onChange={(e) => {
              const w = Number(e.target.value);
              setWait(w);
              newChat(config, w);
            }}
          >
            {WAITS.map((w) => (
              <option key={w} value={w}>
                wait {w}s
              </option>
            ))}
          </select>
          <button onClick={() => newChat()}>New chat</button>
          <button className="primary" onClick={burst}>
            Run burst demo
          </button>
        </div>
      </header>

      <main>
        <section className="chat">
          <div className="feed">
            {feed.map((ev, i) => renderEvent(ev, i))}
            <div ref={feedEnd} />
          </div>

          <form
            className="composer"
            onSubmit={(e) => {
              e.preventDefault();
              send(user, text);
              setText("");
            }}
          >
            <select value={user} onChange={(e) => setUser(e.target.value)}>
              {USERS.map((u) => (
                <option key={u}>{u}</option>
              ))}
            </select>
            <input value={text} onChange={(e) => setText(e.target.value)} placeholder="Message the group… (mention jj to ask the bot)" />
            <button type="submit">Send</button>
          </form>
        </section>

        {openThread != null ? (
          <aside className="thread-pane">
            <div className="thread-head">
              <h2>Thread #{openThread}</h2>
              <button onClick={() => setOpenThread(null)} title="Back to slots">
                ×
              </button>
            </div>
            <div className="thread-feed">
              {threadFeed.map((ev, i) => renderEvent(ev, i, true))}
              <div ref={threadEnd} />
            </div>
            <form
              className="composer"
              onSubmit={(e) => {
                e.preventDefault();
                send(user, threadText, openThread);
                setThreadText("");
              }}
            >
              <select value={user} onChange={(e) => setUser(e.target.value)}>
                {USERS.map((u) => (
                  <option key={u}>{u}</option>
                ))}
              </select>
              <input
                ref={threadInput}
                value={threadText}
                onChange={(e) => setThreadText(e.target.value)}
                placeholder="Reply in thread… (no jj needed)"
              />
              <button type="submit">Send</button>
            </form>
          </aside>
        ) : (
        <aside className="slots">
          <h2>Slots</h2>
          {slotList.length === 0 && <p className="muted">Nothing yet.</p>}
          {slotList.map((s) => (
            <div key={s.n} className={`slot st-${s.status}`}>
              <div className="row">
                <b>#{s.n}</b> <span className="muted">{s.user}</span>
                <span className="badge">{s.kind}</span>
                <span className="badge status">{s.delivered_at != null ? "delivered" : s.status}</span>
                {s.version > 0 && <span className="badge">v{s.version}</span>}
                {s.thread != null && s.thread !== s.n && (
                  <button className="badge link" onClick={() => setOpenThread(s.thread)}>
                    thread #{s.thread}
                  </button>
                )}
              </div>
              <div className="text">{s.text}</div>
              {s.detail && <div className="detail">{s.detail}</div>}
              {s.triage_reason && <div className="muted small">router: {s.triage_reason}</div>}
              {s.brief && <div className="muted small">brief: {s.brief}</div>}
              {s.delivered_at != null && (
                <div className="muted small">
                  wait {(s.delivered_at - s.created).toFixed(1)}s
                  {s.done_at != null &&
                    s.delivered_at - s.done_at > 0.2 &&
                    ` (held ${(s.delivered_at - s.done_at).toFixed(1)}s behind its thread)`}
                </div>
              )}
            </div>
          ))}
        </aside>
        )}
      </main>
    </div>
  );
}
