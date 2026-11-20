+++
date = '2026-10-13'
draft = true
hideCTA = true
title = 'Agent support in otel-desktop-viewer'
type = 'page'
url = '/viewer-agent-support/'
+++

I started building `otel-desktop-viewer` in 2023, and you might have noticed a few changes to the software development life cycle since then.
These days, you’re more likely to ask your agent why `checkout` is slow than look for bottlenecks on a dashboard.

Turns out, agents need feedback just as much as humans do, if not more.
If they can’t see what’s happening in your app, they’re flying blind.
And sure, you can redirect `stdout` to a file for your agent to read or grep.
That works okay for structured logs, but it’s a poor fit for traces and metrics.
Ideally, you (or rather, your agent) should correlate all three to understand what your application is doing.

An agent could try using `otel-desktop-viewer` through the browser UI, but that’s a slow, expensive way to ask for telemetry.

Since `otel-desktop-viewer` already stores your traces, logs and metrics in an embedded DuckDB database, it made sense to extend the CLI with new commands that agents can use to get at the data directly.
Humans get a nice UI, and agents get delicious delicious text.

You don’t even strictly need to know how it works.
Point your OTLP exporter at `otel-desktop-viewer`, then ask your agent what’s slowing your app down:

{{< agent-session
  agent="OpenCode"
  model="Sol 6.2"
  note="Excerpt, formatted for our mutual sanity"
  prompt="Use `otel-desktop-viewer` to find out why checkout is slow and failing in west. Keep it brief."
  command="otel-desktop-viewer query"
  sql=`select
  variant_extract(resource.attributes, 'cloud.region')
    ::varchar as region,
  variant_extract(resource.attributes, 'service.version')
    ::varchar as version,
  count(*) as requests,
  count(*) filter (where status_code = 2) as errors,
  min((end_time - start_time) / 1e6) as min_ms,
  max((end_time - start_time) / 1e6) as max_ms
from spans
where service_name = 'api-gateway'
  and name = 'POST /api/v2/orders'
  and start_time between
    epoch_ns('$OTEL_VIEWER_START'::timestamptz)
    and epoch_ns('$OTEL_VIEWER_END'::timestamptz)
group by 1, 2
order by 1, 2`
  flags=`--endpoint "$OTEL_VIEWER_ENDPOINT" 2>/dev/null`
  output=`region  version  requests  errors  min_ms  max_ms
------  -------  --------  ------  ------  ------
east    1.0.0    16        0       200.0   200.0
west    1.0.0    8         0       200.0   200.0
west    1.1.0    8         4       200.0   1500.0`
>}}
West checkout on gateway v1.1.0 took 200–1,500 ms and failed 4 of 8 times.
All 24 v1.0.0 requests across east and west succeeded in 200 ms.

Payment dominated the slowest traces: a Stripe charge took 1,250 ms in a 1,500 ms success, and a provider timeout returned 504.
Two requests returned 503 with the payment circuit breaker open.
The fourth failure was a separate 402 for insufficient funds.

The slowdown coincided with v1.1.0 in west, but the traces don’t establish that the gateway version caused it.
{{< /agent-session >}}

Let’s look at the new commands, how I made the instructions easy for agents to find, and some fun evals to make sure all this actually works well in real tasks!

## New CLI. Who dis?

The CLI lets your agent query telemetry in a running viewer.
For example, this command returns up to five traces that include a span from `order-service`:

{{< agent-session
  command="otel-desktop-viewer traces --service order-service"
  collapseOutput=true
  flags="--start 2026-10-07T07:54:35Z --end 2026-10-07T08:04:35Z --limit 5"
  output=`traceID                           hasRootSpan  rootSpan                                                    startTime            durationNs  spanCount  errorCount  matchedSpans
--------------------------------  -----------  ----------------------------------------------------------  -------------------  ----------  ---------  ----------  ----------------------------------------------------------------------------
22e5db4c1b2c842b33bde7fa18002fa5  true         {"name":"POST /api/v2/orders","serviceName":"api-gateway"}  1791360015000000000  200000000   6          0           [{"spanID":"2cab9efaf960f4d9","traceID":"22e5db4c1b2c842b33bde7fa18002fa5"}]
a33d995dd3d96d34d60c91fda9154e0d  true         {"name":"POST /api/v2/orders","serviceName":"api-gateway"}  1791360014000000000  200000000   6          0           [{"spanID":"b50ca4771e653c4d","traceID":"a33d995dd3d96d34d60c91fda9154e0d"}]
4da8d5b2168ca6c723efe07641d37a3a  true         {"name":"POST /api/v2/orders","serviceName":"api-gateway"}  1791360013000000000  200000000   6          0           [{"spanID":"b312a879507b2bd5","traceID":"4da8d5b2168ca6c723efe07641d37a3a"}]
37649a40582a99097d2611df7fd218c4  true         {"name":"POST /api/v2/orders","serviceName":"api-gateway"}  1791360012000000000  200000000   6          0           [{"spanID":"97b32d8072134e15","traceID":"37649a40582a99097d2611df7fd218c4"}]
d9dfeb2a6da7bbce535b3a9299c3bfc2  true         {"name":"POST /api/v2/orders","serviceName":"api-gateway"}  1791360011000000000  400000000   5          2           [{"spanID":"f3f0f098c3804d37","traceID":"d9dfeb2a6da7bbce535b3a9299c3bfc2"}]
[5 rows shown; more rows available; use --limit to return more]`
>}}
{{< /agent-session >}}

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

### Skill

The [skill](https://github.com/CtrlSpice/otel-desktop-viewer/blob/main/skills/otel-desktop-viewer/SKILL.md) explains how to use those commands together during an investigation:

- Reusing your running viewer for the investigation and follow-up questions, and stopping only a viewer the agent started.
- Matching questions to commands, using a lookup table.
- Discovering, searching and inspecting telemetry, with examples.
- Running SQL against the installed schema.
- Interpreting results.

To follow these instructions, agents can call the CLI and HTTP endpoints directly with the shell and HTTP tools they already have.
An MCP server would add another interface to configure and maintain; I wanted to test how well the existing interfaces supported an investigation first.

## Discoverability

Your agent needs to be able to find the user manual whether it’s installing the viewer for the first time or using one that’s already running.
So how do we get the right guidance in front of it, whichever workflow it chooses?

By planting the instructions in all the places where agents are likely to trip over them:

1. **Repository README**

   The [repository’s README](https://github.com/CtrlSpice/otel-desktop-viewer) includes installation instructions and a [For agents](https://github.com/CtrlSpice/otel-desktop-viewer#-for-agents) section, so an agent can read the guide before it even installs the viewer or a skill.

2. **CLI help**

   ```sh
   otel-desktop-viewer --help
   ```

   The command list:

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

   The CLI’s `--help` lists `skills` first; that command prints the investigation instructions.
   This makes the guide available from the executable itself, without a separate skill installation.

3. **Viewer homepage**

   The homepage links to the agent instructions at `/llms.txt`.[^luna]
   That makes the guide available over HTTP as well as through the CLI.

4. **Installed skill**

   The installable [skill](https://github.com/CtrlSpice/otel-desktop-viewer/tree/main/skills/otel-desktop-viewer) makes the instructions available in the agent’s own skill library.
   Its description helps the agent recognise relevant tasks and load the guide before contacting the viewer.

**Note:** `skills` and `/llms.txt` serve the same bundled guide, so the instructions match your viewer version.

{{< hire-cta show=true >}}

## Following an investigation

> “Under the most rigorously controlled conditions of pressure, temperature, volume, humidity, and other variables, the organism will do as it damn well pleases.”
>
> — Harvard Law of Animal Behaviour

To test how agents actually use these tools on an incident, I gave GPT-6.1 Sol, Claude Opus 5.5 and Kimi K3.1 Pro access to the same sample shop telemetry and asked each the same question in a fresh conversation:

> Why did checkout slow down and start failing in west while east stayed healthy?

- The viewer was already running with the same sample shop telemetry as the CLI example.
- I supplied the executable’s location, the viewer’s URL and the same 10-minute time window.
- I asked the agents to work independently without starting, stopping or reconfiguring viewers.
- Sol and Kimi ran in OpenCode V2; Opus ran in Claude Code.

I captured the agents’ traces along with their tool inputs and outputs.
Here we can follow Sol’s `trace` command for `checkout-019` to the six spans it returned, including the 1.25-second `stripe/create-charge` call.

{{< video src="/videos/viewer-agent-support/sol-investigation-trace.mp4" poster="/images/viewer-agent-support/sol-investigation-video-opening.png" width="1440" height="900" alt="Silent recording following Sol’s saved investigation from Evals Results to Tool calls and its trace. A correlated log shows the recorded trace command for checkout-019 and its JSON response. The stripe/create-charge entry has durationNs 1250000000, equivalent to 1.25 seconds. The trace context link returns to the investigation waterfall." >}}

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

   {{< figure src="/images/viewer-agent-support/sol-provider-tool-call.png" width="1440" height="924" alt="Sol’s expanded result waterfall beside its recorded trace command in Evals. It inspects trace 8190e585b2d994551a5b5fddb8e9d340 with JSON output. The recorded tool status is completed and tool.is_error is false." >}}

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

{{< figure src="/images/viewer-agent-support/sol-answer-and-grade.png" width="1440" height="964" alt="Sol’s expanded result waterfall beside its recorded answer and grader explanation in Evals. The answer compares west’s 837.5 ms incident mean and 4/8 failures with healthy east at 200 ms and no failures. The Pass explanation confirms the provider delay, timeout, circuit-open rejections, separate card decline and distinction between correlation and causation." >}}

All three runs completed without execution errors and passed grading.

All three models separated the `insufficient_funds` card decline from the three availability failures.
They also noted west’s gateway version change without claiming that it caused the provider delay.

Opus recovered from one permission denial and three SQL errors in 13 calls.
The selected retry uses `resource` in place of the nonexistent `resource_attributes` column.

{{< figure src="/images/viewer-agent-support/opus-query-retry.png" width="1440" height="924" alt="Opus’s result waterfall shows a failed SQL call before the selected successful retry. The retry’s details show success as true and a query using the resource column." >}}

Opus based its answer on spans and logs; it listed metrics without reading their regional datapoints.

Sol used 19 calls and Kimi used 49, both without tool errors.
Both checked regional metric datapoints alongside the spans and logs.
Kimi also read more command help, checked database schemas and cross-checked results with SQL.

{{< figure src="/images/viewer-agent-support/kimi-regional-metrics.png" width="1440" height="1004" alt="Kimi’s result waterfall shows 49 tool calls alongside its recorded payment.provider.duration lookup. The expanded regional series has cloud.region set to west." >}}

The counts include all the agent’s tools, and a shell call can run several viewer commands.
They aren’t a direct measure of model efficiency.

For this example, I ran each model once, with the viewer already running and the executable, URL and time window in the prompt.

To check how consistently agents can answer different questions about this telemetry, I also run a broader eval suite.
I repeat each question and have Sol grade every answer in a fresh conversation against the sample’s known data.

*[Screenshot placeholder: broader eval results across models and repeated questions.]*

## What’s next

Next, I want to get that “experimental” badge off the Evals viewer and get it into your hands.

## Try it out

The CLI and agent instructions are available in [v0.7.0](https://github.com/CtrlSpice/otel-desktop-viewer/releases/tag/v0.7.0).
[Install `otel-desktop-viewer`](https://github.com/CtrlSpice/otel-desktop-viewer#getting-started) and send it telemetry from your service.
Then give your agent the viewer’s URL and ask it to investigate.

[^luna]: Luna 6 is a gremlin that does what it wants.
    It likes to skip the CLI and hit the API directly, so I put instructions at `/llms.txt` to stop it making shit up.
