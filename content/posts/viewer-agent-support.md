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
For example, this command returns summaries for up to five traces that contain a span from `order-service`:

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

I wanted investigations to be incremental, so the agent can choose which records deserve a closer look before [spending tokens on the details](https://www.anthropic.com/engineering/writing-tools-for-agents).
The steps below show how much detail the agent can ask for, from a list of services down to the events on a single span.
The raw output from `otel-desktop-viewer` is there if you want to see how the vegan sausage is made.

{{< cli-flow id="failed-order-walkthrough" >}}
{{< cli-step label="List services" >}}
{{< agent-session
  command="otel-desktop-viewer services"
  flags="--start 2026-10-07T07:54:35Z --end 2026-10-07T08:04:35Z"
  collapseOutput=true
  output=`serviceNamespace  namespaceKind  serviceName        nameKind  traceCount  spanCount  errorSpanCount  logCount  errorLogCount  metricCount  dataPointCount  exemplarCount  lastSeen
----------------  -------------  -----------------  --------  ----------  ---------  --------------  --------  -------------  -----------  --------------  -------------  -------------------
                  string         api-gateway        string    32          32         4               33        4              6            192             1              1791360275000000000
                  string         catalog-service    string    32          32         0               0         0              0            0               0              1791360015005000000
                  string         inventory-service  string    32          32         0               1         0              0            0               0              1791360015030000000
                  string         order-service      string    32          32         0               0         0              0            0               0              1791360015055000000
                  string         payment-service    string    32          62         6               33        4              1            48              0              1791360275000000000
                  string         worker             string    0           0          0               1         0              0            0               0              1791360000000000000`
>}}
`order-service` appears in 32 traces in the sample’s ten-minute window.
{{< /agent-session >}}
{{< /cli-step >}}

{{< cli-step label="Inspect attributes" >}}
{{< agent-session
  command="otel-desktop-viewer attributes keys"
  flags="--start 2026-10-07T07:54:35Z --end 2026-10-07T08:04:35Z"
  collapseOutput=true
  output=`key                        kind    foundOn
-------------------------  ------  ----------------------------------------
customer.id                string  [{"signal":"traces","ownerType":"span"}]
error.type                 string  [{"signal":"traces","ownerType":"span"}]
http.request.method        string  [{"signal":"traces","ownerType":"span"}]
http.response.status_code  int64   [{"signal":"traces","ownerType":"span"}]
http.route                 string  [{"signal":"traces","ownerType":"span"}]
order.sequence             int64   [{"signal":"traces","ownerType":"span"}]
order.sequence             string  [{"signal":"traces","ownerType":"span"}]
request.id                 string  [{"signal":"traces","ownerType":"span"}]
server.address             string  [{"signal":"traces","ownerType":"span"}]
timeout.ms                 int64   [{"signal":"traces","ownerType":"span"}]`
>}}
{{< /agent-session >}}

{{< agent-session
  command="otel-desktop-viewer attributes values error.type"
  flags="--start 2026-10-07T07:54:35Z --end 2026-10-07T08:04:35Z"
  collapseOutput=true
  output=`value                   kind    count  percentage
----------------------  ------  -----  ----------
"PaymentDeclinedError"  string  1      50.00%
"TimeoutError"          string  1      50.00%`
>}}
{{< /agent-session >}}
{{< /cli-step >}}

{{< cli-step label="Search traces" >}}
{{< agent-session
  command="otel-desktop-viewer traces --service order-service"
  flags="--start 2026-10-07T07:59:59Z --end 2026-10-07T08:00:01Z --limit 5"
  collapseOutput=true
  output=`traceID                           hasRootSpan  rootSpan                                                    startTime            durationNs  spanCount  errorCount  matchedSpans
--------------------------------  -----------  ----------------------------------------------------------  -------------------  ----------  ---------  ----------  ----------------------------------------------------------------------------
e5b61bd360c791ebfc51d6ef2702ad06  true         {"name":"POST /api/v2/orders","serviceName":"api-gateway"}  1791360000000000000  200000000   6          0           [{"spanID":"6590892552e5de56","traceID":"e5b61bd360c791ebfc51d6ef2702ad06"}]
eb962ed9c467935016892a6590c45aa1  true         {"name":"POST /api/v2/orders","serviceName":"api-gateway"}  1791360000000000000  1200000000  6          3           [{"spanID":"3639c543a09ef105","traceID":"eb962ed9c467935016892a6590c45aa1"}]`
>}}
Two traces match this two-second window.
One took 1.2 seconds and has three spans in error.
{{< /agent-session >}}
{{< /cli-step >}}

{{< cli-step label="Read trace" >}}
{{< agent-session
  command="otel-desktop-viewer trace eb962ed9c467935016892a6590c45aa1"
  collapseOutput=true
  output=`TRACE
traceID                           spanCount  logCount  startTime            durationNs
--------------------------------  ---------  --------  -------------------  ----------
eb962ed9c467935016892a6590c45aa1  6          5         1791360000000000000  1200000000

SPANS
spanID            parentSpanID      service            name                     startOffsetNs  durationNs
----------------  ----------------  -----------------  -----------------------  -------------  ----------
be5e36333c06a1a5  NULL              api-gateway        POST /api/v2/orders      0              1200000000
eb4021cd7d3803e8  be5e36333c06a1a5  catalog-service    catalog/resolve-items    25000000       255000000
8849de4699f52c82  be5e36333c06a1a5  inventory-service  inventory/reserve-stock  285000000      215000000
3639c543a09ef105  be5e36333c06a1a5  order-service      order/create             505000000      275000000
d47e503bcd05acaf  be5e36333c06a1a5  payment-service    payment/charge           785000000      265000000
1438f83fe7dd7271  d47e503bcd05acaf  payment-service    stripe/create-charge     882000000      118000000

TRACE LOGS
timestamp            spanID            severity  service            eventName  body
-------------------  ----------------  --------  -----------------  ---------  --------------------------------------------
1791360000490000000  8849de4699f52c82  WARN      inventory-service             Stock below threshold; reservation succeeded
1791360000950000000  1438f83fe7dd7271  ERROR     payment-service               Payment declined: insufficient_funds
1791360001200000000  be5e36333c06a1a5  ERROR     api-gateway                   POST /api/v2/orders returned 402
1791360001250000000  NULL              INFO      api-gateway                   Request audit recorded
1791360001400000000  52e5df3f2b41bd07  INFO      payment-service               Delayed payment audit received`
>}}
The trace’s logs include a payment error on the `stripe/create-charge` span.
{{< /agent-session >}}
{{< /cli-step >}}

{{< cli-step label="Inspect span" >}}
{{< agent-session
  command="otel-desktop-viewer span eb962ed9c467935016892a6590c45aa1"
  flags="1438f83fe7dd7271"
  collapseOutput=true
  output=`SPAN
traceID                           spanID            parentSpanID      service          name                  traceState  flags  resourceSchemaURL                        scopeSchemaURL                           scopeName             scopeVersion  resourceDropped  scopeDropped
--------------------------------  ----------------  ----------------  ---------------  --------------------  ----------  -----  ---------------------------------------  ---------------------------------------  --------------------  ------------  ---------------  ------------
eb962ed9c467935016892a6590c45aa1  1438f83fe7dd7271  d47e503bcd05acaf  payment-service  stripe/create-charge              0      https://opentelemetry.io/schemas/1.34.0  https://opentelemetry.io/schemas/1.34.0  shop.instrumentation  1.0.0         0                0

TIMING AND STATUS
startTime            endTime              durationNs  kindCode  kind    statusCodeValue  status  statusMessage                      droppedAttributes  droppedEvents  droppedLinks
-------------------  -------------------  ----------  --------  ------  ---------------  ------  ---------------------------------  -----------------  -------------  ------------
1791360000882000000  1791360001000000000  118000000   3         Client  2                Error   Card declined: insufficient funds  0                  0              0

RESOURCE ATTRIBUTES
key              kind    value
---------------  ------  ---------------
cloud.region     string  west
service.name     string  payment-service
service.version  string  1.0.0

SCOPE ATTRIBUTES
key  kind  value
---  ----  -----

SPAN ATTRIBUTES
key             kind    value
--------------  ------  --------------------
error.type      string  PaymentDeclinedError
server.address  string  api.stripe.com

EVENTS (1)
timestamp            name       droppedAttributes  attributes
-------------------  ---------  -----------------  ------------------------------------------------------------------------------------------------------------
1791360000950000000  exception  0                  [exception.message (string)=Card declined: insufficient funds, exception.type (string)=PaymentDeclinedError]

LINKS (0)
traceID  spanID  traceState  flags  droppedAttributes  attributes
-------  ------  ----------  -----  -----------------  ----------

CORRELATED LOGS (1)
logRef                                timestamp            observedTimestamp    traceID                           spanID            severityText  severityNumber  service          eventName  bodyKind  body                                  flags  droppedAttributes  resourceSchemaURL                        scopeSchemaURL                           scopeName             scopeVersion  resourceDropped  scopeDropped  attributes  resourceAttributes                                                                                   scopeAttributes
------------------------------------  -------------------  -------------------  --------------------------------  ----------------  ------------  --------------  ---------------  ---------  --------  ------------------------------------  -----  -----------------  ---------------------------------------  ---------------------------------------  --------------------  ------------  ---------------  ------------  ----------  ---------------------------------------------------------------------------------------------------  ---------------
7013cac4-9d2d-4176-807f-4b5debf71929  1791360000950000000  1791360000950500000  eb962ed9c467935016892a6590c45aa1  1438f83fe7dd7271  ERROR         17              payment-service             string    Payment declined: insufficient_funds  0      0                  https://opentelemetry.io/schemas/1.34.0  https://opentelemetry.io/schemas/1.34.0  shop.instrumentation  1.0.0         0                0             []          [cloud.region (string)=west, service.name (string)=payment-service, service.version (string)=1.0.0]  []`
>}}
The exception on that span is a `PaymentDeclinedError`: “Card declined: insufficient funds”.
{{< /agent-session >}}
{{< /cli-step >}}
{{< /cli-flow >}}

### SQL

Cool, but what if your agent only needs the error type from that `stripe/create-charge` span?

First it needs to know where to look.
The `query` command accepts read-only SQL, so the agent can use `SHOW TABLES` to find the `spans` table and `DESCRIBE spans` to find the `attributes` column.
Then it can ask for that one field without pulling the whole-ass span into context. 🎉

From there, it can ask DuckDB a broader question: how many `stripe/create-charge` calls failed in each region?

{{< cli-flow id="sql-schema-walkthrough" label="Query payment spans" >}}
{{< cli-step label="Find the spans table" >}}
{{< agent-session
  command="otel-desktop-viewer query"
  sql="SHOW TABLES"
  showSQL=true
  flags="--limit 50"
  collapseOutput=true
  output=`name
------------------------
eval_datasets
eval_evidence
eval_experiment_indexes
eval_experiment_rows
eval_experiment_search
eval_experiments
eval_imports
eval_parts
eval_records
eval_transfers
events
exemplars
ingest_rejections
links
logs
metric_datapoints
metric_records
metric_series
metrics
schema_meta
search_owners
search_paths
search_postings
search_service_resources
search_services
search_values
spans`
>}}
The table list includes `spans`.
{{< /agent-session >}}
{{< /cli-step >}}

{{< cli-step label="Find span attributes" >}}
{{< agent-session
  command="otel-desktop-viewer query"
  sql="DESCRIBE spans"
  showSQL=true
  collapseOutput=true
  output=`column_name               column_type                                                                                                                                                                    null  key   default  extra
------------------------  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------  ----  ----  -------  -----
trace_id                  UUID                                                                                                                                                                           NO    PRI   NULL     NULL
trace_state               VARCHAR                                                                                                                                                                        YES   NULL  NULL     NULL
span_id                   UBIGINT                                                                                                                                                                        NO    PRI   NULL     NULL
parent_span_id            UBIGINT                                                                                                                                                                        YES   NULL  NULL     NULL
flags                     UINTEGER                                                                                                                                                                       YES   NULL  NULL     NULL
name                      VARCHAR                                                                                                                                                                        YES   NULL  NULL     NULL
kind                      INTEGER                                                                                                                                                                        YES   NULL  NULL     NULL
start_time                UBIGINT                                                                                                                                                                        YES   NULL  NULL     NULL
end_time                  UBIGINT                                                                                                                                                                        YES   NULL  NULL     NULL
resource                  STRUCT(id UUID, payload_id UUID, schema_url VARCHAR, attributes VARIANT, dropped_attributes_count UINTEGER)                                                                    NO    NULL  NULL     NULL
scope                     STRUCT(id UUID, "name" VARCHAR, "version" VARCHAR, schema_url VARCHAR, attributes VARIANT, dropped_attributes_count UINTEGER)                                                  NO    NULL  NULL     NULL
attributes                VARIANT                                                                                                                                                                        NO    NULL  NULL     NULL
dropped_attributes_count  UINTEGER                                                                                                                                                                       YES   NULL  NULL     NULL
dropped_events_count      UINTEGER                                                                                                                                                                       YES   NULL  NULL     NULL
dropped_links_count       UINTEGER                                                                                                                                                                       YES   NULL  NULL     NULL
status_code               INTEGER                                                                                                                                                                        YES   NULL  NULL     NULL
status_message            VARCHAR                                                                                                                                                                        YES   NULL  NULL     NULL
service_name              VARCHAR                                                                                                                                                                        NO    NULL  ''       NULL
events                    STRUCT(id UUID, "name" VARCHAR, "timestamp" UBIGINT, attributes VARIANT, dropped_attributes_count UINTEGER, ordinal UINTEGER)[]                                                YES   NULL  NULL     NULL
links                     STRUCT(id UUID, linked_trace_id UUID, linked_span_id UBIGINT, trace_state VARCHAR, attributes VARIANT, dropped_attributes_count UINTEGER, flags UINTEGER, ordinal UINTEGER)[]  YES   NULL  NULL     NULL`
>}}
The `attributes` column holds the span attributes, including `error.type`.
{{< /agent-session >}}
{{< /cli-step >}}

{{< cli-step label="Read the error type" >}}
{{< agent-session
  command="otel-desktop-viewer query"
  showSQL=true
  sql=`select
  attributes['error.type']::varchar as error_type
from spans
where
  trace_id_wire(trace_id) = 'eb962ed9c467935016892a6590c45aa1'
  and span_id_wire(span_id) = '1438f83fe7dd7271'`
  collapseOutput=true
  output=`error_type
--------------------
PaymentDeclinedError`
>}}
The agent gets one value back: `PaymentDeclinedError`.
{{< /agent-session >}}
{{< /cli-step >}}

{{< cli-step label="Compare regions" >}}
{{< agent-session
  prompt="How many `stripe/create-charge` calls failed in each region?"
  command="otel-desktop-viewer query"
  showSQL=true
  sql=`select
  resource.attributes['cloud.region']::varchar as region,
  count(*) as calls,
  count(*) filter (where status_code = 2) as failed
from spans
where name = 'stripe/create-charge'
group by region
order by region`
  collapseOutput=true
  output=`region  calls  failed
------  -----  ------
east    16     0
west    14     2`
>}}
Two of 14 calls failed in west; none of the 16 in east failed.
DuckDB counts the spans and sends back one row per region.
{{< /agent-session >}}
{{< /cli-step >}}
{{< /cli-flow >}}

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
