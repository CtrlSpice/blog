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
If they can’t see what’s happening in your app, they’re flying blind.
You can save stdout to a file for your agent to read or grep.
That works okay, but it’s a poor fit for traces, metrics and structured logs.
You should be using all three to understand what your application is doing.

otel-desktop-viewer already stores that telemetry in an embedded DuckDB database you can query with SQL.
I’ve extended it to support coding agents, with new CLI commands they can run to investigate that same data.
Let’s look at these new tools, how agents find them, and how three models used the viewer to investigate an incident.

## A CLI

The CLI lets your agent query telemetry in a running viewer.
First, though, it needs to know which commands are available and how to use them.

It can find the instructions in a few places:

1. **Installing the viewer**

   The [repository’s README](https://github.com/CtrlSpice/otel-desktop-viewer) includes installation instructions and a [For agents](https://github.com/CtrlSpice/otel-desktop-viewer#-for-agents) section on how to use the viewer.

   > Install otel-desktop-viewer using the instructions at `https://github.com/CtrlSpice/otel-desktop-viewer`.
   > Start it locally and tell me its URL when it’s ready to receive telemetry.

2. **Instrumenting your service**

   Your agent can use the viewer to check what your service emits as it adds instrumentation.
   The CLI’s `--help` lists `skills` first; that command prints the investigation instructions.

   > Add OpenTelemetry traces, metrics and logs to this service and configure it to export to my running otel-desktop-viewer.
   > The viewer’s UI is at `http://localhost:8000`.
   > Run a test request and check that the viewer received the resulting telemetry.

3. **Investigating telemetry in a running viewer**

   Once telemetry is in the viewer, you can share its URL along with whatever you want investigated.
   The CLI sends queries to that address.
   To find the instructions, your agent can fetch the homepage, whose headers and HTML both link to `/llms.txt` and the JSON-RPC API description at `/openrpc.json`.

   > Checkout has become slow.
   > Use my running otel-desktop-viewer at `http://localhost:8000` to investigate the last 30 minutes.
   > Find which operations account for the delay and link to the traces that support your answer.

   We’ll [follow an investigation](#following-an-investigation) from this starting point later in the post.

4. **Using an installed skill**

   You can also install the [skill](https://github.com/CtrlSpice/otel-desktop-viewer/tree/main/skills/otel-desktop-viewer) in your coding agent.
   Its description helps the agent recognise relevant tasks and load the full instructions before contacting the viewer.

   > Use the otel-desktop-viewer skill to check `order-service` for errors over the last 30 minutes.
   > My viewer is running at `http://localhost:8000`.
   > Summarise the different failures and link to an example trace for each.

The CLI’s `skills` command and `/llms.txt` return the same bundled `SKILL.md`.
I maintain that file in one place, so both return instructions that match the viewer version you’re running.

### Help

From the command list, your agent can open a particular command’s `--help` for its arguments, defaults and examples.
For a slow `checkout` request, `traces --help` shows example searches and the flags for filtering by service or changing the time window.

{{< figure src="/images/viewer-agent-support/traces-help.png" alt="Help for otel-desktop-viewer traces on a dark background. Examples show searching all traces, filtering to checkout over the last 30 minutes, and requesting JSON for an explicit time range. Flag descriptions include the defaults: the last hour, at most 25 summaries and http://localhost:8000." >}}

### Skill

The [skill](https://github.com/CtrlSpice/otel-desktop-viewer/blob/main/skills/otel-desktop-viewer/SKILL.md) explains how to use those commands together during an investigation:

- Reusing your running viewer for the investigation and follow-up questions, and stopping only a viewer the agent started.
- Matching questions to commands, using a lookup table.
- Discovering, searching and inspecting telemetry, with examples.
- Running SQL against the installed schema.
- Interpreting results.

To follow these instructions, agents can call the CLI and HTTP endpoints directly with the shell and HTTP tools they already have.
An MCP server would add another interface to configure and maintain; I wanted to test how well the existing interfaces supported an investigation first.

### Incremental responses

The approach is incremental: start with summaries, then request more detail as the investigation narrows.
Search commands return summaries that your agent can use to choose what to inspect next.
It can then request a trace’s spans and logs, followed by a single span’s full details, keeping [results proportionate to the question](https://www.anthropic.com/engineering/writing-tools-for-agents).

To choose a starting point, `services` lists what your application has emitted, grouped by service name with counts of spans, logs, metric datapoints and errors:

```sh
otel-desktop-viewer services
```

To find useful filters, your agent can inspect the available attributes.
`attributes keys` lists their names and types, while `attributes values` shows the values recorded for a key and how often each occurs:

```sh
otel-desktop-viewer attributes keys
otel-desktop-viewer attributes values <key>
```

Attribute discovery defaults to span attributes from the last hour, with flags to inspect other signals or change the time window.

Once it knows which service to check, it can search for matching traces.
Search commands default to at most 25 summaries from the last hour and print tables; `--json` requests structured output instead.

For example, with the viewer’s sample shop telemetry loaded, an investigation into failed orders can start with a search for `order-service` across the sample’s 10-minute window:

```sh
otel-desktop-viewer traces --service order-service \
  --start 2026-10-07T07:54:35Z --end 2026-10-07T08:04:35Z --json
```

One matching summary is `checkout-017`, a 1.2-second trace with six spans, three of them in error:

```json
{
  "traceID": "eb962ed9c467935016892a6590c45aa1",
  "rootSpan": {
    "serviceName": "api-gateway",
    "name": "POST /api/v2/orders"
  },
  "durationNs": "1200000000",
  "spanCount": 6,
  "errorCount": 3
}
```

To locate those three errors, `trace` fetches compact rows for all of the trace’s stored spans and correlated logs:

```sh
otel-desktop-viewer trace eb962ed9c467935016892a6590c45aa1 --json
```

Among the log previews is this error on span `1438f83fe7dd7271`:

```json
{
  "spanID": "1438f83fe7dd7271",
  "severity": "ERROR",
  "service": "payment-service",
  "body": "Payment declined: insufficient_funds"
}
```

The trace’s span list identifies `1438f83fe7dd7271` as `stripe/create-charge`.
With the failure narrowed down to that operation, `span` fetches its attributes, events and correlated logs:

```sh
otel-desktop-viewer span eb962ed9c467935016892a6590c45aa1 \
  1438f83fe7dd7271 --json
```

The full record has an `error.type` of `PaymentDeclinedError` and an exception event with the message “Card declined: insufficient funds”.
Your agent can report that specific failure with its trace and span IDs.
If you want to inspect the request yourself, it can link to `/traces/<trace-id>` in your running viewer, where you can explore the waterfall and spans.

Logs and metrics follow the same progression from summaries to full records.
`logs` and `metrics` return search summaries, and `log` fetches a complete log record.
To read a metric’s datapoints, your agent can use `metric` to list its series, then select one with `--series`.

### SQL

When the built-in commands don’t fit the question, `query` lets your agent run read-only SQL and make it DuckDB’s problem.
For an aggregation, DuckDB does the calculation and returns the result rows instead of every contributing record.

`query` also accepts `SHOW TABLES` and `DESCRIBE`, so the agent can learn the installed schema before writing SQL:

```sh
otel-desktop-viewer query 'SHOW TABLES'
otel-desktop-viewer query 'DESCRIBE spans'
```

{{< hire-cta show=true >}}

## Following an investigation

> “Under the most rigorously controlled conditions of pressure, temperature, volume, humidity, and other variables, the organism will do as it damn well pleases.”
>
> — Harvard Law of Animal Behaviour

To test how agents actually use these tools on an incident, I gave GPT-6.1 Sol, Claude Opus 5.5 and Kimi K3.1 Pro the same question, each in a fresh conversation:

> Why did checkout slow down and start failing in west while east stayed healthy?

The viewer was already running with the same sample shop telemetry as the CLI example.
I supplied the executable’s location, the viewer’s URL and the inclusive time window from `2026-10-07T07:54:35Z` to `2026-10-07T08:04:35Z`.
I asked the agents to work independently without starting, stopping or reconfiguring viewers.
Sol and Kimi ran in OpenCode V2; Opus ran in Claude Code.

We can follow Sol’s investigation through excerpts from its 19 recorded tool calls.
The command excerpts below omit the run’s temporary executable path and `--endpoint` value.

1. **Finding the data**

   Sol read `--help` and `skills`, ran `SHOW TABLES` and `services`, and used `DESCRIBE spans` to check the span schema.
   After listing logs and metrics in the supplied time window, it sent this SQL to `query --limit 150 --json` to compare gateway and payment spans across regions:

   ```sql
   select trace_id_wire(trace_id) trace, name, service_name,
          to_timestamp(start_time/1000000000.0) t,
          (end_time-start_time)/1000000.0 ms,
          status_code, status_message, attributes,
          resource.attributes resource
   from spans
   where start_time between 1791359675000000000 and 1791360275000000000
     and service_name in ('api-gateway','payment-service')
   order by start_time
   ```

   The `start_time` bounds are the prompt’s window as Unix timestamps in nanoseconds.
   `trace_id_wire` formats each stored UUID as the hex trace ID the CLI commands accept.
   In the returned rows, `cloud.region` and `service.version` live on `resource.attributes`, while request labels such as `checkout-020` come from the gateway span’s `request.id` attribute.

   From those rows, Sol could compare the two regions over time.
   West’s `api-gateway` changed from version `1.0.0` to `1.1.0` at 08:00 UTC, when checkouts there started slowing down and failing.
   East’s gateway stayed on `1.0.0` with 200 ms checkouts.
   `payment-service` stayed on `1.0.0` in both regions.

2. **Separating the failures**

   Four west checkouts failed from 08:00 UTC, returning three different HTTP statuses.
   Sol checked the `checkout.duration` and `payment.provider.duration` metric series, then inspected their schema with `DESCRIBE metric_datapoints` before opening the failed traces.

   It started with `checkout-020`:

   ```sh
   otel-desktop-viewer trace \
     ca87e8521a08e87bf49d914613f69aa6 --json
   ```

   The payment-provider span took 2000 ms, and its correlated log reported “Payment provider timed out”.
   The whole checkout request took 2200 ms and returned HTTP 504.

   The next two west requests, `checkout-021` and `checkout-022`, returned HTTP 503.
   Sol’s earlier `logs` search had shown circuit-breaker rejection previews for both, so it fetched the full structured log for `checkout-021`.
   The body contained `reason="circuit_open"` and `provider.attempted=false`.
   `payment-service` had rejected both requests immediately without calling the provider.

   That left the earlier failure on `checkout-017` at 08:00 UTC.
   Sol opened the trace and found HTTP 402 with “Payment declined: insufficient_funds”, separating a card decline from the three availability failures.

3. **Checking the regional totals**

   With the individual failures explained, Sol queried datapoints for `checkout.duration`, `checkout.failures` and `payment.provider.duration` to check the totals across both regions.
   For west from 08:00 UTC, the histograms matched the trace and log evidence:

   - **Checkout:** 8 requests totalling 6.7 seconds, for an 837.5 ms mean.
   - **Provider:** 6 attempts, because the open circuit breaker rejected the other 2 orders before they reached the provider.

   Combining those metric datapoints with the span rows supported the regional breakdown in Sol’s final answer:

   | Region | Period | Failed / total | Mean duration |
   | --- | --- | ---: | ---: |
   | east | before 08:00 | 0 / 8 | 200 ms |
   | east | from 08:00 | 0 / 8 | 200 ms |
   | west | before 08:00 | 0 / 8 | 200 ms |
   | **west** | **from 08:00** | **4 / 8** | **837.5 ms** |

4. **Checking the slow successful requests**

   West’s 837.5 ms mean also included slow successful checkouts, so Sol opened two to see where their time went.
   In `checkout-018`, the request took 600 ms while the payment-provider call took its baseline 80 ms.
   The extra time was spread across the catalog, inventory, order and payment spans, so provider waiting wasn’t the only source of delay.

   In `checkout-019`, however, most of the 1500 ms request sat inside a 1250 ms payment-provider call:

   ```sh
   otel-desktop-viewer trace \
     8190e585b2d994551a5b5fddb8e9d340 --json
   ```

   {{< figure src="/images/viewer-agent-support/sol-provider-tool-call.png" width="1440" height="754" alt="Close-up of Sol’s recorded trace command in Evals. It inspects trace 8190e585b2d994551a5b5fddb8e9d340 with JSON output. The recorded tool status is completed and tool.is_error is false." >}}

   | Operation | Duration |
   | --- | ---: |
   | POST /api/v2/orders | 1500 ms |
   | catalog/resolve-items | 20 ms |
   | inventory/reserve-stock | 20 ms |
   | order/create | 20 ms |
   | payment/charge | 1350 ms |
   | ↳ **stripe/create-charge** | **1250 ms** |

   {{< figure src="/images/viewer-agent-support/sol-provider-tool-result.png" width="1440" height="924" alt="Sol’s expanded investigation tree alongside the recorded trace result in Evals. Five spans are folded and stripe/create-charge is expanded. The provider span has durationNs 1250000000, equivalent to 1250 ms." >}}

   For its final tool call, Sol returned to `checkout-020` and fetched the full provider span to check the deadline configuration.
   That span’s attributes included `timeout.ms=2000`, and its exception event recorded “Payment provider deadline exceeded after 2000 ms”.

In its final answer, Sol cited trace IDs so you could open the relevant requests in the viewer.
It noted west’s gateway version change but didn’t claim that the change caused the provider calls to stall.

By `checkout-023` and `checkout-024`, west checkouts were back to HTTP 200 in 200 ms with 80 ms provider calls.
Sol noted that the window contained no later checkouts to confirm sustained recovery.

*[Working source for both examples: updated `testdata/otlp/small` on `test/release-installation-evals`; add the source link when that revision lands.]*

### Comparing the three investigations

I recorded the tool calls and final answers, then asked Sol to grade each investigation in a fresh conversation against the sample’s known timings and failure modes.
To pass, an answer had to compare west’s slowdown and failures with healthy east, trace most of the delay to payment-provider calls, and explain the timeout followed by the circuit-open rejections.
It also had to separate the card decline from the availability failures and avoid inventing an unsupported root cause for the provider delay.

{{< figure src="/images/viewer-agent-support/sol-answer-and-grade.png" width="1440" height="1360" alt="Sol’s recorded answer and expanded grader explanation in Evals. The answer compares west’s 837.5 ms incident mean and 4/8 failures with healthy east at 200 ms and no failures. The Pass explanation confirms the provider delay, timeout, circuit-open rejections, separate card decline and distinction between correlation and causation." >}}

All three runs completed without execution errors and passed grading.

{{< figure src="/images/viewer-agent-support/three-investigations-overview.png" width="1440" height="884" alt="Three recorded experiments in Evals: Claude Opus 5.5 in Claude Code, Kimi K3.1 Pro in OpenCode V2, and GPT-6.1 Sol in OpenCode V2. Opus is selected, with its expanded investigation tree showing model requests and tool calls." >}}

All three models separated the `insufficient_funds` card decline from the three availability failures.
They also noted west’s gateway version change without claiming that it caused the provider delay.

The tool-call totals include every tool in each agent’s harness, and a single shell call can run several viewer commands.
Those counts don’t rank model efficiency on their own, but the recorded calls show how the investigations differed.

Opus made 13 tool calls and hit four errors: Claude Code denied one compound shell command, one SQL query referenced a nonexistent `resource_attributes` column, and two queries passed UUIDs to `substr` without casting them to strings.
After switching to a simpler shell command and correcting its SQL, it supported its answer with spans and logs.
It listed metrics but didn’t inspect their regional datapoints.

Sol used 19 tool calls and Kimi used 49, both without tool errors.
They checked regional metric datapoints alongside the traces and logs.
Kimi’s extra calls included additional `--help` and schema reads, individual span and metric-series lookups, and SQL cross-checks.

All three used the CLI and SQL to investigate this dataset successfully.
I ran each model once, with the viewer already running and the executable, URL and time window in the prompt.
I need repeated trials on this and other incidents to measure how consistently they succeed.

## What’s next

Next, I want to ask agents to install the viewer and test each way of finding the instructions.
I’ll also ask follow-up questions to check that they keep using the same running viewer.

## Try it out

The CLI and agent instructions are available in [v0.7.0](https://github.com/CtrlSpice/otel-desktop-viewer/releases/tag/v0.7.0).
[Install otel-desktop-viewer](https://github.com/CtrlSpice/otel-desktop-viewer#getting-started) and send it telemetry from your service.
Then give your agent the viewer’s URL and ask it to investigate.
