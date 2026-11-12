+++
date = '2026-10-13'
draft = true
title = 'Untitled.'
type = 'page'
url = '/viewer-agent-support/'
+++

So agents, amirite?
They’ve fundamentally changed how we [write and observe software](https://www.honeycomb.io/blog/what-comes-after-observability), and we’re all scrambling to figure this out.
There are some very clever people writing about what this all means for the [future of observability](https://clickhouse.com/blog/the-future-of-observability-not-one-proprietary-ai-agent-thousands-by-teams).

I’ve been adding agent support to [OpenTelemetry Desktop Viewer](https://github.com/CtrlSpice/otel-desktop-viewer) because more of us now ask an agent to investigate, then maybe show us what it found so we can poke around.
I want to encourage that curiosity and make the viewer a good experience for our new robot friends too.

## A CLI

The viewer collects OpenTelemetry data and stores it locally in DuckDB.
Agents can query a running viewer through the CLI, while you can explore the same telemetry in the browser.

Let’s say an agent is meeting your telemetry for the first time.
[`services`](https://github.com/CtrlSpice/otel-desktop-viewer/pull/588 "services_command.go") shows which services have sent telemetry.
The `attributes` commands list [keys](https://github.com/CtrlSpice/otel-desktop-viewer/pull/582 "attributes_command.go") and the [values](https://github.com/CtrlSpice/otel-desktop-viewer/pull/583 "attributes_command.go") recorded for a given key.
By default, attribute discovery looks at span attributes from the last hour:

```sh
otel-desktop-viewer services
otel-desktop-viewer attributes keys
otel-desktop-viewer attributes values <key>
```

With a service name, the agent can find recent traces and use a returned trace ID to [inspect a trace](https://github.com/CtrlSpice/otel-desktop-viewer/pull/576 "trace_command.go, span_command.go") in the terminal:

```sh
otel-desktop-viewer traces --service checkout --since 30m
otel-desktop-viewer trace <trace-id>
```

The agent can use that ID to give you a [link to the trace](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.6.0/desktopexporter/internal/frontend/src/route/router.ts) at `/traces/<trace-id>`, where you can explore its waterfall and spans.

Logs and metrics follow the same pattern: [plural commands](https://github.com/CtrlSpice/otel-desktop-viewer/pull/568 "telemetry_search.go") (`logs`, `metrics`) search, and [singular commands](https://github.com/CtrlSpice/otel-desktop-viewer/pull/591 "log_command.go, metric_command.go") (`log`, `metric`) inspect one record.

When the built-in commands don’t answer the question, an agent can use [`query`](https://github.com/CtrlSpice/otel-desktop-viewer/pull/565 "query_command.go") to run read-only SQL and make it DuckDB’s problem.
`query` also takes [`SHOW TABLES` and `DESCRIBE`](https://github.com/CtrlSpice/otel-desktop-viewer/pull/566 "query_command.go"), so the agent can learn the schema before writing a query:

```sh
otel-desktop-viewer query 'SHOW TABLES'
otel-desktop-viewer query 'DESCRIBE spans'
```

Search commands return compact tables by default, with `--json` for machine-readable results that preserve exact values.
The aim is to give the agent enough information to investigate while keeping the [output reasonably token-efficient](https://www.anthropic.com/engineering/writing-tools-for-agents).

## Help

An agent discovering the viewer on its own can use [`--help`](https://github.com/CtrlSpice/otel-desktop-viewer/pull/570 "root_command.go") to find the commands and learn how to call them:

```sh
otel-desktop-viewer --help
otel-desktop-viewer traces --help
```

The top-level help lists the commands:

```text
COMMANDS
  skills    🧠 Print the agent usage guide
  logs      🪵 Search logs in a running viewer
  log       🪵 Inspect one complete log
  metrics   📈 Search metrics in a running viewer
  metric    📈 Inspect one Metric or its series datapoints
  traces    🧵 Search traces in a running viewer
  trace     🧵 Inspect one complete trace
  span      🧵 Inspect one span
  attributes 🏷️ Discover attribute keys and values
  services  Discover services and their telemetry counts
  query     🦆 Run SQL against a running viewer
  export    📤 Export stored telemetry as OTLP JSON
```

`traces --help` explains its flags and gives examples to copy.
Help works without a running viewer, so an agent can learn what the tool does before starting it.

## Skills

Help tells an agent how to call a command.
The [usage guide](https://github.com/CtrlSpice/otel-desktop-viewer/pull/567 "skills/otel-desktop-viewer/SKILL.md") explains how to connect to the viewer and choose commands for an investigation.
It recommends built-in commands when they answer the question and SQL when they don’t.

The guide also asks agents to distinguish received values from calculations, so a rate or quantile the viewer computed doesn’t get reported as something your service sent.
It sets rules for starting and stopping the viewer too:

- Reuse a running viewer when possible; keep any viewer the agent starts as a foreground child process.
- Keep using the same viewer for follow-up questions.
- Stop only a viewer the agent started.

A viewer that’s already running is probably yours, and you’re probably in the middle of something.

I keep one guide, with three ways for an agent to get it:

- `otel-desktop-viewer skills` prints the guide bundled with the executable, even when the viewer isn’t running.
- `/llms.txt` serves the guide from a running viewer, so an agent can fetch it with just the viewer’s URL.
- `npx skills add CtrlSpice/otel-desktop-viewer --skill otel-desktop-viewer` installs it as an agent skill, available before the first connection.

The [CLI](https://github.com/CtrlSpice/otel-desktop-viewer/pull/580 "skills/guide.go") and [HTTP](https://github.com/CtrlSpice/otel-desktop-viewer/pull/581 "skills/guide.go") copies ship with the viewer, so the instructions match the installed version.

Agents can also query the viewer’s JSON-RPC API directly over HTTP.
The root response includes a `Link` header pointing to both the guide and an [OpenRPC API description](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.6.0/desktopexporter/internal/server/openrpc.go), which lists the methods and parameters.

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
Start with the [CLI usage guide](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.6.0/skills/otel-desktop-viewer/SKILL.md).

I’ll get into the evals in more detail in the next post.
