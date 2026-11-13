const $ = (id) => document.getElementById(id);
let active = "",
  snapshot = null,
  lastBatch = null,
  page = 0,
  busy = false;
const number = (n) =>
  Number(n).toLocaleString("en-US", { maximumFractionDigits: 6 });
function el(tag, text, cls) {
  const n = document.createElement(tag);
  if (text !== undefined) n.textContent = text;
  if (cls) n.className = cls;
  return n;
}
async function api(url, body) {
  const response = await fetch(
    url,
    body === undefined
      ? {}
      : {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
  );
  const data = await response.json();
  if (!response.ok) throw Error(data.error || `HTTP ${response.status}`);
  return data;
}
const path = (action = "") =>
  `/api/streams/${active}${action ? "/" + action : ""}`;
async function task(fn) {
  if (busy) return;
  busy = true;
  const controls = [
    ...document.querySelectorAll("button,input,select,textarea"),
  ].map((e) => [e, e.disabled]);
  controls.forEach(([e]) => (e.disabled = true));
  try {
    await fn();
  } catch (error) {
    $("status").textContent = error.message;
  } finally {
    busy = false;
    controls.forEach(([e, disabled]) => {
      if (e.isConnected) e.disabled = disabled;
    });
    $("retry").disabled = !lastBatch;
    paginateButtons();
  }
}
function nextID() {
  $("event-id").value = `event-${crypto.randomUUID()}`;
}
async function refresh() {
  const data = await api("/api/streams");
  $("stream").replaceChildren();
  if (!data.streams.length) {
    active = "";
    $("stream").append(new Option("Choose a stream", ""));
    $("empty").hidden = false;
    $("workspace").hidden = true;
    return;
  }
  if (!data.streams.some((s) => s.name === active))
    active = data.streams[0].name;
  for (const stream of data.streams)
    $("stream").append(new Option(stream.name, stream.name));
  $("stream").value = active;
  $("empty").hidden = true;
  await reload();
}
async function reload() {
  snapshot = await api(path());
  $("workspace").hidden = false;
  render();
}
function render() {
  const s = snapshot.stream;
  $("watermark-value").textContent = `${number(s.watermark)} s`;
  $("config").textContent =
    `${s.width}-second windows · ${s.lateness}-second lateness allowance`;
  $("watermark-input").value = s.watermark + s.width;
  $("watermark-input").min = s.watermark;
  $("revision").textContent = `Revision ${s.revision}`;
  $("counts").replaceChildren(
    ...[
      ["on-time", "On-time receipts"],
      ["late", "Accepted late"],
      ["expired", "Expired receipts"],
    ].map(([key, label]) => {
      const block = el("div");
      block.append(
        el("strong", number(snapshot.counts[key])),
        el("span", label),
      );
      return block;
    }),
  );
  renderWindows();
  renderRecent();
  $("selection").hidden = true;
}
function filtered() {
  if (!snapshot) return [];
  const text = $("filter").value.toLowerCase(),
    state = $("state-filter").value;
  return snapshot.windows.filter(
    (w) =>
      w.key.toLowerCase().includes(text) &&
      (state === "all" || w.state === state),
  );
}
function paginateButtons() {
  const total = filtered().length;
  $("prev").disabled = busy || page === 0;
  $("next").disabled = busy || (page + 1) * 50 >= total;
}
function renderWindows() {
  const rows = filtered();
  page = Math.min(page, Math.max(0, Math.ceil(rows.length / 50) - 1));
  const table = el("table"),
    head = el("tr");
  head.append(
    ...["Event-time window", "Key", "Count", "Sum", "Average", "State"].map(
      (x) => el("th", x),
    ),
  );
  const thead = el("thead");
  thead.append(head);
  table.append(thead);
  const body = el("tbody");
  for (const w of rows.slice(page * 50, page * 50 + 50)) {
    const row = el("tr"),
      cell = el("td"),
      button = el("button", `[${w.start}, ${w.end})`);
    button.setAttribute(
      "aria-label",
      `Inspect ${w.key} window ${w.start} to ${w.end}`,
    );
    button.addEventListener("click", () => showWindow(w));
    cell.append(button);
    row.append(
      cell,
      ...[w.key, number(w.count), number(w.total), number(w.average)].map((x) =>
        el("td", x),
      ),
    );
    const state = el("td");
    state.append(el("span", w.state, `state ${w.state}`));
    row.append(state);
    body.append(row);
  }
  table.append(body);
  $("windows").replaceChildren(table);
  if (!rows.length)
    $("windows").append(
      el("p", "No accepted windows match this view.", "hint"),
    );
  $("window-count").textContent =
    `${number(rows.length)} / ${number(snapshot.windows.length)} keyed windows`;
  $("page").textContent =
    `Page ${page + 1} of ${Math.max(1, Math.ceil(rows.length / 50))}`;
  paginateButtons();
}
function showWindow(w) {
  $("selection").hidden = false;
  $("selection").replaceChildren(
    el("strong", `${w.key} · [${w.start}, ${w.end}) seconds`),
    el(
      "p",
      `${w.count} accepted events, including ${w.late_count} received behind the watermark. Minimum ${number(w.minimum)}, maximum ${number(w.maximum)}.`,
    ),
    el(
      "p",
      `Grace starts at ${w.end} s. Final at watermark ${w.final_at} s. Current watermark: ${snapshot.stream.watermark} s. Expired receipts never change this aggregate.`,
    ),
  );
}
function renderRecent() {
  const table = el("table"),
    head = el("tr");
  head.append(
    ...[
      "Sequence",
      "Event ID",
      "Key",
      "Event time",
      "Value",
      "Decision",
      "Arrival watermark",
    ].map((x) => el("th", x)),
  );
  const thead = el("thead");
  thead.append(head);
  table.append(thead);
  const body = el("tbody");
  for (const e of snapshot.recent) {
    const row = el("tr");
    row.append(
      ...[
        e.sequence,
        e.event_id,
        e.key,
        e.time,
        number(e.value),
        e.disposition,
        e.watermark,
      ].map((x) => el("td", String(x))),
    );
    body.append(row);
  }
  table.append(body);
  $("recent").replaceChildren(table);
}
async function ingest(events) {
  const response = await api(path("events"), { events });
  lastBatch = { stream: active, events: structuredClone(events) };
  $("receipt").textContent =
    `${response.created} new receipts · ${response.duplicates} duplicate retries. ${response.receipts.map((r) => `${r.id}: ${r.disposition}${r.duplicate ? " (original receipt)" : ""}`).join("; ")}`;
  $("status").textContent =
    `Batch committed atomically at stream revision ${response.revision}.`;
  await reload();
}
function download(name, content, type) {
  const url = URL.createObjectURL(new Blob([content], { type })),
    a = el("a");
  a.href = url;
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
$("create-form").addEventListener("submit", (e) => {
  e.preventDefault();
  task(async () => {
    const created = await api("/api/streams", {
      name: $("name").value.trim(),
      width: Number($("width").value),
      lateness: Number($("lateness").value),
    });
    active = created.name;
    lastBatch = null;
    page = 0;
    $("receipt").textContent = "";
    $("status").textContent =
      `Created ${active}. Window settings are fixed for this stream.`;
    $("create-details").open = false;
    await refresh();
  });
});
$("demo").addEventListener("click", () =>
  task(async () => {
    const name = `example-${Date.now()}`;
    await api("/api/streams", { name, width: 60, lateness: 30 });
    active = name;
    await api(path("events"), {
      events: [
        { id: "arrival-1", key: "north", time: 10, value: 12 },
        { id: "arrival-2", key: "south", time: 35, value: 7 },
        { id: "arrival-3", key: "north", time: 65, value: 18 },
        { id: "arrival-4", key: "south", time: 82, value: 9 },
        { id: "arrival-5", key: "north", time: 122, value: 15 },
        { id: "arrival-6", key: "south", time: 176, value: 11 },
      ],
    });
    $("status").textContent =
      "Example created. Advance to 60 s, then ingest an event at 15 s to inspect the grace period.";
    await refresh();
  }),
);
$("stream").addEventListener("change", () =>
  task(async () => {
    active = $("stream").value;
    lastBatch = null;
    page = 0;
    $("receipt").textContent = "";
    $("audit-result").textContent = "";
    await reload();
    $("status").textContent = `Loaded ${active}.`;
  }),
);
$("refresh").addEventListener("click", () =>
  task(async () => {
    await refresh();
    $("status").textContent = "Loaded the latest durable state.";
  }),
);
$("watermark-form").addEventListener("submit", (e) => {
  e.preventDefault();
  task(async () => {
    await api(path("watermark"), {
      watermark: Number($("watermark-input").value),
      expected_watermark: snapshot.stream.watermark,
    });
    await reload();
    $("status").textContent =
      `Watermark advanced to ${snapshot.stream.watermark} s. Final windows reject new arrivals; duplicate receipts retain their original decision.`;
  });
});
$("event-form").addEventListener("submit", (e) => {
  e.preventDefault();
  task(async () => {
    await ingest([
      {
        id: $("event-id").value.trim(),
        key: $("event-key").value.trim(),
        time: Number($("event-time").value),
        value: Number($("event-value").value),
      },
    ]);
    nextID();
  });
});
$("retry").addEventListener("click", () =>
  task(async () => {
    if (lastBatch?.stream !== active)
      throw Error("Choose the stream that received this batch.");
    await ingest(lastBatch.events);
  }),
);
$("send-batch").addEventListener("click", () =>
  task(async () => {
    const events = JSON.parse($("batch").value);
    if (!Array.isArray(events))
      throw Error("Batch JSON must be an array of events.");
    await ingest(events);
  }),
);
for (const id of ["filter", "state-filter"])
  $(id).addEventListener(id === "filter" ? "input" : "change", () => {
    page = 0;
    renderWindows();
  });
$("prev").addEventListener("click", () => {
  page--;
  renderWindows();
});
$("next").addEventListener("click", () => {
  page++;
  renderWindows();
});
for (const id of ["audit", "repair"])
  $(id).addEventListener("click", () =>
    task(async () => {
      const r = await api(
        path(id === "audit" ? "audit" : "rebuild"),
        id === "repair" ? {} : undefined,
      );
      $("audit-result").textContent =
        `${r.matched_before ? "Projection matches receipts." : r.repaired ? "Projection rebuilt from receipts." : "Projection differs from receipts."} Checked ${r.receipt_count} receipts and ${r.windows} keyed windows.`;
      if (r.repaired) await reload();
    }),
  );
$("export").addEventListener("click", () =>
  task(async () => {
    const log = await api(path("export"));
    download(
      `${active}-events.json`,
      JSON.stringify(log, null, 2) + "\n",
      "application/json",
    );
    $("status").textContent =
      "Full event receipts and watermark history exported.";
  }),
);
$("export-windows").addEventListener("click", () => {
  const header = [
    "start",
    "end",
    "key",
    "count",
    "total",
    "average",
    "minimum",
    "maximum",
    "late_count",
    "state",
    "final_at",
  ];
  download(
    `${active}-windows.csv`,
    [
      header.join(","),
      ...snapshot.windows.map((w) => header.map((k) => w[k]).join(",")),
    ].join("\n") + "\n",
    "text/csv",
  );
});
$("open-batch").addEventListener("click", () => $("file").click());
$("file").addEventListener("change", () =>
  task(async () => {
    const file = $("file").files[0];
    try {
      if (!file) return;
      if (file.size > 63000) throw Error("Batch files must be at most 63 KB.");
      const data = JSON.parse(await file.text());
      if (!Array.isArray(data) || data.length < 1 || data.length > 200)
        throw Error("Expected an array of 1–200 events.");
      $("batch").value = JSON.stringify(data, null, 2);
      $("status").textContent =
        "Batch staged for review. Choose Ingest batch to commit it.";
    } finally {
      $("file").value = "";
    }
  }),
);
nextID();
task(refresh);
