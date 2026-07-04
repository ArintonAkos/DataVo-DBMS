const statusDot = document.querySelector("#statusDot");
const statusText = document.querySelector("#statusText");
const queryList = document.querySelector("#queryList");
const events = document.querySelector("#events");
const pauseButton = document.querySelector("#pauseButton");
const clearButton = document.querySelector("#clearButton");

let socket;
let paused = false;

function connect() {
  const protocol = location.protocol === "https:" ? "wss" : "ws";
  socket = new WebSocket(`${protocol}://${location.host}/ws`);

  socket.addEventListener("open", () => {
    statusDot.classList.add("connected");
    statusText.textContent = "Connected";
  });

  socket.addEventListener("close", () => {
    statusDot.classList.remove("connected");
    statusText.textContent = "Disconnected";
    setTimeout(connect, 1000);
  });

  socket.addEventListener("message", event => {
    const message = JSON.parse(event.data);
    if (message.type === "hello") {
      renderQueries(message.queries);
      renderMetrics(message.metrics);
      return;
    }

    if (message.type === "metrics") {
      renderMetrics(message.metrics);
      return;
    }

    if (message.type === "change") {
      appendEvent(message.id, {
        tick: message.tick,
        added: message.added.length,
        removed: message.removed.length,
        updated: message.updated.length,
        sample: message.added[0] || message.updated[0] || message.removed[0] || null
      });
      return;
    }

    appendEvent(message.type, message);
  });
}

function renderQueries(queries) {
  queryList.replaceChildren();
  for (const query of queries) {
    const card = document.createElement("div");
    card.className = "query-card";

    const title = document.createElement("h3");
    title.textContent = query.label;

    const code = document.createElement("code");
    code.textContent = query.sql;

    const button = document.createElement("button");
    button.type = "button";
    button.textContent = "Subscribe";
    button.addEventListener("click", () => {
      send({ type: "subscribe", id: query.id, sql: query.sql });
    });

    card.append(title, code, button);
    queryList.appendChild(card);
  }
}

function renderMetrics(metrics) {
  document.querySelector("#tick").textContent = metrics.tick ?? 0;
  document.querySelector("#mutations").textContent = metrics.mutations ?? 0;
  document.querySelector("#deltaRows").textContent = metrics.deltaRows ?? 0;
  document.querySelector("#subscriptions").textContent = metrics.subscriptions ?? 0;
  pauseButton.textContent = metrics.running ? "Pause" : "Resume";
  paused = !metrics.running;
}

function appendEvent(label, payload) {
  const node = document.createElement("div");
  node.className = "event";

  const strong = document.createElement("strong");
  strong.textContent = label;

  const pre = document.createElement("pre");
  pre.textContent = JSON.stringify(payload, null, 2);

  node.append(strong, pre);
  events.prepend(node);
  while (events.children.length > 80) {
    events.lastElementChild.remove();
  }
}

function send(payload) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(payload));
  }
}

pauseButton.addEventListener("click", () => {
  send({ type: paused ? "resume" : "pause" });
});

clearButton.addEventListener("click", () => {
  events.innerHTML = "";
});

connect();
