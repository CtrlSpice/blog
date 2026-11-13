+++
date = '2026-10-13'
draft = true
title = 'Agent support in otel-desktop-viewer'
type = 'page'
url = '/viewer-agent-support/'
+++

So agents, amirite?
They’ve fundamentally changed how we create and observe software, and now we're all scrambling to figure it out.
Very clever people out there are writing about the [ways agents are changing investigations](https://www.honeycomb.io/blog/what-comes-after-observability) and what it all means for the [future of observability](https://clickhouse.com/blog/the-future-of-observability-not-one-proprietary-ai-agent-thousands-by-teams).
I’m excited to see where we take all this.

I’ve always wanted [otel-desktop-viewer](https://github.com/CtrlSpice/otel-desktop-viewer) to be nice to use, and that includes our new robot friends.
I’ve added a CLI and a usage guide so you can investigate with your own agent.

## A CLI

The way we investigate software has shifted: we ask an agent for an answer, then ask it to show us what it found when we want to poke around.
The CLI queries a running viewer through its API, using the same telemetry you explore in the browser.

An agent meeting your telemetry for the first time needs to learn what’s there before choosing filters.
The [`services` command](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/services_command.go) lists services with counts of their spans, logs and metric datapoints.
The [attribute commands](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/attributes_command.go) list keys with their types, then count the values recorded for a chosen key.
That gives the agent something to query without guessing what you instrumented:

```sh
otel-desktop-viewer services
otel-desktop-viewer attributes keys
otel-desktop-viewer attributes values <key>
```

Attribute discovery defaults to span attributes from the last hour.
For example, an agent investigating failed orders can look for traces that include `order-service`.
With the viewer’s [sample shop telemetry](https://github.com/CtrlSpice/otel-desktop-viewer/tree/v0.7.0/testdata/otlp/small) loaded, it can search the sample’s time window:

```sh
otel-desktop-viewer traces --service order-service \
  --start 2026-10-07T07:54:35Z --end 2026-10-07T08:04:35Z --json
```

The CLI prints tables by default; `--json` returns structured results.
One result is a 1.2-second trace with six spans, three of them in error.
Here’s an excerpt from its summary:

```json
{
  "traceID": "00000000000000000000000000000011",
  "rootSpan": {
    "serviceName": "api-gateway",
    "name": "POST /api/v2/orders"
  },
  "durationNs": "1200000000",
  "spanCount": 6,
  "errorCount": 3
}
```

The [`trace` overview](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/trace_command.go) then shows compact rows for all of that trace’s stored spans and linked logs:

```sh
otel-desktop-viewer trace 00000000000000000000000000000011 --json
```

Among the log previews is this error, excerpted here:

```json
{
  "spanID": "0000000000000006",
  "severity": "ERROR",
  "service": "payment-service",
  "body": "Payment declined: insufficient_funds"
}
```

The span list identifies that span as `stripe/create-charge`.
The agent can use [`span`](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/span_command.go) to request its attributes, events and correlated logs:

```sh
otel-desktop-viewer span 00000000000000000000000000000011 \
  0000000000000006 --json
```

That detail includes an `error.type` of `PaymentDeclinedError` and an exception event with the message “Card declined: insufficient funds”.
The agent now has a specific error to report, with the trace and span IDs you can use to explore it.

I don’t want a search to fill the agent’s context with every attribute on every span.
[Search commands](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/telemetry_search.go) default to at most 25 summaries from the last hour, with flags to change the limit and time window.
Keeping the [results proportionate to the question](https://www.anthropic.com/engineering/writing-tools-for-agents) leaves the detailed records for when the agent needs them.

The viewer’s [trace routes](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/desktopexporter/internal/server/server.go#L169-L177) let the agent give you a link at `/traces/<trace-id>`, where you can explore its waterfall and spans.
You can follow the parts that interest you, with the same trace that the agent was investigating.

Logs and metrics follow a similar pattern: `logs` and `metrics` search, while `log` returns a complete log and `metric` lists a metric’s series.
Selecting a series returns its datapoints.

When the built-in commands don’t answer the question, an agent can use [`query`](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/query_command.go) to run read-only SQL and make it DuckDB’s problem.
For an aggregation, DuckDB can do the calculation and return the answer rows without sending every contributing record to the agent.
`query` also takes `SHOW TABLES` and `DESCRIBE`, so the agent can learn the installed schema before writing SQL:

```sh
otel-desktop-viewer query 'SHOW TABLES'
otel-desktop-viewer query 'DESCRIBE spans'
```

## Help

An agent arriving with just the executable can use [`--help`](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/root_command.go) to find those commands and learn how to call them:

```sh
otel-desktop-viewer --help
otel-desktop-viewer traces --help
```

The top-level help lists the commands; each command’s help explains its flags and gives examples to copy.
For `traces`, that includes the default time window and result limit, so the agent can see what its search will cover before making a request.
Help works without a running viewer, so an agent can learn what the tool does before starting it.

## Skills

Help tells an agent how to call a command.
The [usage guide](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/skills/otel-desktop-viewer/SKILL.md) explains how to connect to the viewer and choose commands for an investigation.
It recommends built-in commands when they answer the question and SQL when they don’t.

The guide also asks agents to distinguish received values from calculations, so a rate or quantile the viewer computed doesn’t get reported as something your service sent.
For example, a trace’s duration is calculated from the earliest span start and latest span end; the overview gives the agent that duration alongside the individual span timings.

A viewer that’s already running is probably yours, and you’re probably in the middle of something.
The guide sets rules for sharing it:

- Reuse a running viewer when possible; keep any viewer the agent starts as a foreground child process.
- Keep using the same viewer for follow-up questions.
- Stop only a viewer the agent started.

I want the agent to find these instructions wherever it encounters the viewer, so I keep one guide with three ways to get it:

- `otel-desktop-viewer skills` prints the guide bundled with the executable, even when the viewer isn’t running.
- `/llms.txt` serves the guide from a running viewer, so an agent can fetch it with just the viewer’s URL.
- `npx skills add CtrlSpice/otel-desktop-viewer --skill otel-desktop-viewer` installs it as an agent skill, available before the first connection.

The CLI and HTTP copies come from the same [file embedded in the executable](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/skills/guide.go), so they ship with the version of the viewer that the agent is using.

An agent approaching over HTTP can also query the viewer’s JSON-RPC API directly.
The response to `GET /` includes a `Link` header pointing to both the guide and a machine-readable [OpenRPC API description](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/desktopexporter/internal/server/openrpc.go), with methods, parameters and result formats.
The viewer’s URL is enough to find the instructions for calling it.

## Checking that it worked

> Under the most rigorously controlled conditions of pressure, temperature, volume, humidity, and other variables, the organism will do as it damn well pleases.
>
> — Harvard Law of Animal Behaviour.

Agents, like the rest of us, do as they damn well please, so I ran evals to see whether they could find the commands and guide, then use them during an investigation.

**[PLACEHOLDER: how the evals were set up, in a sentence or two.]**

**[PLACEHOLDER: eval screenshot. Does the agent find the command it needs through `--help`?]**

**[PLACEHOLDER: eval screenshot. Does the agent pick up the usage guide?]**

**[PLACEHOLDER: eval screenshot. Does the agent use a built-in command when one answers the question, and SQL when it doesn’t?]**

**[PLACEHOLDER: eval screenshot. Does the agent keep received values apart from calculated ones?]**

**[PLACEHOLDER: eval screenshot. Does the agent reuse a running viewer, and stop only one it started?]**

**[PLACEHOLDER: what the results say, in a sentence or two.]**

## Try it out

Agent support is out now.
[Install otel-desktop-viewer](https://github.com/CtrlSpice/otel-desktop-viewer#getting-started) and give your agent the [usage guide](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/skills/otel-desktop-viewer/SKILL.md) to get started.

I’ll get into the evals in more detail in the next post.
