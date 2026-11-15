+++
date = '2026-10-13'
draft = true
hideCTA = true
title = 'Agent support in otel-desktop-viewer'
type = 'page'
url = '/viewer-agent-support/'
+++

I started building otel-desktop-viewer in 2023, and you might have noticed a few changes to the software development life cycle since then.
These days, you’re more likely to ask your agent why `checkout` is slow than look for bottlenecks on a dashboard.

Turns out, agents need feedback just as much as humans do, if not more.
Without being able to see what’s happening in your app, they’re flying blind.
You can save stdout to a file for your agent to read or grep, but querying OpenTelemetry data directly lets it follow connections between signals without reading through so much output.

otel-desktop-viewer already collects that telemetry in an embedded DuckDB database, keeping your traces, metrics and logs together in a local columnar store you can query with SQL.
I’ve extended otel-desktop-viewer to support coding agents, with new CLI tools they can call to investigate your application’s telemetry.

With the viewer running and receiving data, give your agent its URL and ask it to investigate.
For a closer look at the evidence, ask for an example trace and open it in the [viewer’s UI](/viewer-ui-improvements/).

Let’s look at the new tools, how agents find them, and how I tested that they can use the viewer to investigate an incident.

## A CLI

Your agent can query the telemetry in a running viewer and give you a link to a trace to explore in the browser.

The agent can start by finding out which services have sent data and which attributes are available to search.
`services` lists services with counts of their spans, logs and metric datapoints.
`attributes keys` lists attribute names and types; `attributes values` shows their values and how often they occur:

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

The `trace` command then shows compact rows for all of that trace’s stored spans and linked logs:

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
The agent can use `span` to request its attributes, events and correlated logs:

```sh
otel-desktop-viewer span 00000000000000000000000000000011 \
  0000000000000006 --json
```

That detail includes an `error.type` of `PaymentDeclinedError` and an exception event with the message “Card declined: insufficient funds”.
The agent now has a specific error to report, with the trace and span IDs you can use to explore it.

I don’t want a search to fill the agent’s context with every attribute on every span.
Search commands default to at most 25 summaries from the last hour, with flags to change the limit and time window.
Keeping the [results proportionate to the question](https://www.anthropic.com/engineering/writing-tools-for-agents) leaves the detailed records for when the agent needs them.

If you want to see that trace for yourself, the agent can link you to `/traces/<trace-id>` in your running viewer.
You can explore its waterfall and spans, following the parts that interest you.

Logs and metrics follow a similar pattern: `logs` and `metrics` search, while `log` returns a complete log and `metric` lists a metric’s series.
Selecting a series returns its datapoints.

When the built-in commands don’t answer the question, an agent can use `query` to run read-only SQL and make it DuckDB’s problem.
For an aggregation, DuckDB can do the calculation and return the answer rows without sending every contributing record to the agent.
`query` also takes `SHOW TABLES` and `DESCRIBE`, so the agent can learn the installed schema before writing SQL:

```sh
otel-desktop-viewer query 'SHOW TABLES'
otel-desktop-viewer query 'DESCRIBE spans'
```

## Discoverability

Onboarding sets the tone for how a tool gets used.
With agents, this means chunking instructions into manageable pieces and unceremoniously shoving them anywhere one is likely to look.

Where they start depends on what you already have and what you ask them to do:

1. **Installing the viewer**

   The [repository’s README](https://github.com/CtrlSpice/otel-desktop-viewer) covers installation for people and agents alike, but the instructions for using the viewer differ.
   The [For agents](https://github.com/CtrlSpice/otel-desktop-viewer#-for-agents) section points to command help and explains how to read the skill through the CLI or `/llms.txt`.

2. **Instrumenting your service**

   Your agent can check telemetry in the viewer as they instrument your service.
   When they look up how to use the CLI, `--help` lists `skills` first; running that command prints the instructions.
   Individual commands have their own help with arguments, defaults and examples.

3. **Investigating telemetry in a running viewer**

   Once your telemetry is in the viewer, you can share its URL along with whatever you want investigated.
   A request to that URL returns links in both the headers and HTML to the same instructions at `/llms.txt` and the JSON-RPC API description at `/openrpc.json`.

4. **Using an installed skill**

   You can also install the [skill](https://github.com/CtrlSpice/otel-desktop-viewer/tree/main/skills/otel-desktop-viewer) in your coding agent.
   Its description helps them recognise tasks the viewer can help with and read the full instructions before they ever contact it.

The CLI and `/llms.txt` serve that same `SKILL.md`, bundled with the viewer.
I update the advice in one place, and both copies match the version you’re running.

## Instructions for agents

Once they’ve found the instructions, I want them to get on with the investigation without reading the whole manual first.

### Help

Your agent can start with the command list, then open a particular command’s `--help` for more detail.
For a slow `checkout` request, `traces --help` includes example searches and explains which flags filter by service or change the time window.

{{< figure src="/images/viewer-agent-support/traces-help.png" alt="Help for otel-desktop-viewer traces on a dark background. Examples show searching all traces, filtering to checkout over the last 30 minutes, and requesting JSON for an explicit time range. Flag descriptions include the defaults: the last hour, at most 25 summaries and http://localhost:8000." >}}

### Skill

The [skill](https://github.com/CtrlSpice/otel-desktop-viewer/blob/main/skills/otel-desktop-viewer/SKILL.md) helps your agent choose commands for an investigation:

- Reusing your running viewer for the investigation and follow-up questions, and stopping only a viewer the agent started.
- Matching questions to commands, using a lookup table.
- Discovering, searching and inspecting telemetry, with examples.
- Running SQL against the installed schema.
- Interpreting results.

The CLI works for people, scripts and agents, using the same commands.
Agents already have shell and HTTP tools, so they can use these interfaces directly.
An MCP server would add another interface to configure and maintain; I wanted to test how well the existing ones supported an investigation first.

{{< hire-cta show=true >}}

## Following an investigation

Checkout is slow and orders are failing in west; east is healthy.
You ask your agent to investigate.
The viewer’s [sample shop telemetry](https://github.com/CtrlSpice/otel-desktop-viewer/tree/main/testdata/otlp/small) gives us a handy example of that incident.

The [UI post](/viewer-ui-improvements/) showed how to inspect traces and logs in the browser; here, we follow the evidence through the CLI.

1. **Regional comparison**

   The first query compares requests by region before and after 08:00 UTC:

   | Region | Period | Failed / total | Mean duration |
   | --- | --- | ---: | ---: |
   | east | baseline | 0 / 8 | 200 ms |
   | east | incident | 0 / 8 | 200 ms |
   | west | baseline | 0 / 8 | 200 ms |
   | **west** | **incident** | **4 / 8** | **837.5 ms** |

2. **Payment-provider latency**

   East stays steady while west gets slower and starts failing, so the next step is to inspect a slow west request.
   In `checkout-019`, the payment-provider call takes 1250 ms, compared with 80 ms in healthy traces:

   ```sh
   otel-desktop-viewer trace \
     8190e585b2d994551a5b5fddb8e9d340
   ```

   | Operation | Duration |
   | --- | ---: |
   | POST /api/v2/orders | 1500 ms |
   | catalog/resolve-items | 20 ms |
   | inventory/reserve-stock | 20 ms |
   | order/create | 20 ms |
   | payment/charge | 1350 ms |
   | ↳ **stripe/create-charge** | **1250 ms** |

3. **Timeouts and circuit-breaker rejections**

   The provider call accounts for most of that request’s duration, but the failed requests need a closer look.
   In `checkout-020`, the provider times out after 2000 ms and checkout returns HTTP 504.
   The next two requests return 503 because the circuit breaker is open; their logs record `provider.attempted=false`, so neither reaches the provider.

   That explains three of the four failures.
   The fourth, `checkout-017`, is a card decline for insufficient funds (HTTP 402), a separate failure from the provider timeout.

   By `checkout-023/024`, provider calls are back to 80 ms and both requests return 200.

4. **Regional metrics**

   Those traces show individual requests slowing down, failing and recovering.
   The west-region histograms provide a cross-check over the same interval:

   - **Checkout:** 8 requests, 6.7 seconds total → 837.5 ms mean.
   - **Provider:** 6 attempts; the circuit breaker rejected the other 2 orders.

The answer includes trace IDs for follow-up in the viewer.
The underlying cause of the provider delay still needs investigation.

*[Working source: upgraded shared dataset on `test/release-installation-evals`; update the source link when that revision lands.]*

## Checking whether it works

> “Under the most rigorously controlled conditions of pressure, temperature, volume, humidity, and other variables, the organism will do as it damn well pleases.”
>
> — Harvard Law of Animal Behaviour

I gave GPT-6.1 Sol, Claude Opus 5.5 and Kimi K3.1 Pro the regional checkout investigation above, each in a fresh conversation.
Sol and Kimi ran in OpenCode V2; Opus ran in Claude Code.
I recorded their tool calls and answers, then had a separate fresh Sol conversation grade each investigation against the sample’s known timings and failures, using the recorded evidence.

All three passed:

| Model | Grade | Tool calls | Tool errors |
| --- | --- | ---: | ---: |
| GPT-6.1 Sol | Pass | 19 | 0 |
| Claude Opus 5.5 | Pass | 13 | 4 |
| Kimi K3.1 Pro | Pass | 49 | 0 |

Each answer compared west’s slowdown and failures with healthy east, traced the delay to payment-provider calls, and explained the timeout followed by circuit-breaker rejections.
They also separated the insufficient-funds decline from the availability failures.
All three noticed the gateway version change in west without claiming that its timing proved it caused the incident.

The grades alone don’t show how they got there.
Opus recovered from four tool errors and supported its answer with subsequent successful queries.
Kimi used 49 tool calls to reach a passing answer, compared with Sol’s 19.
The Evals view lets me inspect those calls alongside the answers and grades.

**[App screenshot: Evals results overview.]**

**[App screenshot: investigation answer, assessment and supporting tool result.]**

That’s one attempt per model on one incident.
It shows that all three could use the viewer to investigate this sample, but it doesn’t tell me how consistently they’ll succeed on other incidents.
Installation, separate instruction-discovery cases and viewer reuse across follow-up questions still need their own tests.

## What’s next

The Evals UI is experimental, and I’m using these runs to test it alongside the agent-facing tools.
I’ll cover how it compares results and connects answers to tool calls in a separate post.

## Try it out

[Install otel-desktop-viewer](https://github.com/CtrlSpice/otel-desktop-viewer#getting-started) and send it telemetry from your service.
Then give your agent the viewer’s URL and ask it to investigate.
The CLI and agent instructions are available in [v0.7.0](https://github.com/CtrlSpice/otel-desktop-viewer/releases/tag/v0.7.0).
