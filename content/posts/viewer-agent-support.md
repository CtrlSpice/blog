+++
date = '2026-10-13'
draft = true
hideCTA = true
title = 'Claude needs a telescope'
summary = 'Agent support in otel-desktop-viewer'
type = 'page'
url = '/claude-needs-a-telescope/'
+++

![Lulu lifts a little orange crab to the telescope, then gives him head pats before he scuttles away with hearts.](/images/viewer-agent-support/lulu-and-crab.gif)

Many moons ago (2023) `otel-desktop-viewer` had a fairly predictable audience: humans writing and debugging code by hand.
As we started handing more of that work to agents, an interface designed for the human visual cortex became a bit awkward.
What good is a trace viewer to an agent?

Turns out, they need feedback just as much as humans do, if not more!
Without that feedback loop an agent is flying blind.
Stopgaps like redirecting `stdout` to a file for your agent to read or grep work okay, but don't play well with traces and metrics.

Ideally, you (or rather, your agent) should be able to correlate all three to understand what your application is doing.
That makes the difference between your agent burning tokens on `printf` lines around a network call and jumping straight to the exact error already captured by OpenTelemetry instrumentation.

An agent could try using `otel-desktop-viewer` through the browser UI, but that’s a slow, expensive way to ask for telemetry.
Since `otel-desktop-viewer` already stores your traces, logs and metrics in an embedded DuckDB database, it made sense to extend the CLI with new commands that agents can use to get at the data directly.
Humans get a nice UI, and agents get delicious delicious text.

You don’t even strictly need to know how it works.
Just ask your agent to use `otel-desktop-viewer` and it’ll figure it out:

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
  output=`traceID                           hasRootSpan  rootSpan                                                    startTime            durationNs  spanCount  errorCount  matchedSpans (row traceID omitted)
--------------------------------  -----------  ----------------------------------------------------------  -------------------  ----------  ---------  ----------  ----------------------------------
22e5db4c1b2c842b33bde7fa18002fa5  true         {"name":"POST /api/v2/orders","serviceName":"api-gateway"}  1791360015000000000  200000000   6          0           [{"spanID":"2cab9efaf960f4d9"}]
a33d995dd3d96d34d60c91fda9154e0d  true         {"name":"POST /api/v2/orders","serviceName":"api-gateway"}  1791360014000000000  200000000   6          0           [{"spanID":"b50ca4771e653c4d"}]
4da8d5b2168ca6c723efe07641d37a3a  true         {"name":"POST /api/v2/orders","serviceName":"api-gateway"}  1791360013000000000  200000000   6          0           [{"spanID":"b312a879507b2bd5"}]
37649a40582a99097d2611df7fd218c4  true         {"name":"POST /api/v2/orders","serviceName":"api-gateway"}  1791360012000000000  200000000   6          0           [{"spanID":"97b32d8072134e15"}]
d9dfeb2a6da7bbce535b3a9299c3bfc2  true         {"name":"POST /api/v2/orders","serviceName":"api-gateway"}  1791360011000000000  400000000   5          2           [{"spanID":"f3f0f098c3804d37"}]
[5 rows shown; more rows available; use --limit to return more]`
>}}
{{< /agent-session >}}

I wanted investigations to be incremental, so the agent can choose which records deserve a closer look before [spending tokens on the details](https://www.anthropic.com/engineering/writing-tools-for-agents).

The CLI gives your agent a compact table by default.
That keeps the token count down for the agent, but you can still read the output if you ever need to follow along.

I counted tokens on synthetic shop data using `o200k_base`, so treat these numbers as a rough guide to what you’d see with your own data and model.
Notice how the gap grows when the same query returns more traces.

{{< compact-table >}}
| Traces | Table tokens | JSON tokens | Token difference |
| ---: | ---: | ---: | ---: |
| 5 | 457 | 1,158 | 701 fewer (60.5%) |
| 10 | 851 | 2,319 | 1,468 fewer (63.3%) |
| 20 | 1,608 | 4,591 | 2,983 fewer (65.0%) |
{{< /compact-table >}}

Still, the biggest token savings come from reading less.
If your agent only needs the IDs of traces with errors, it can use `--json` and filter the results with `jq`.
Out of 20 traces, that leaves four IDs for a whopping 80 tokens.

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

The [skill](https://github.com/CtrlSpice/otel-desktop-viewer/blob/main/skills/otel-desktop-viewer/SKILL.md) points your agent to `otel-desktop-viewer --help` for command syntax, then offers some practical advice for the investigation:

- Reuse the viewer that’s already running and leave it running.
  If you ask the agent to start one, that’s the one it should stop when it’s done.
- If a search comes back empty, check the endpoint and filters before reporting that there’s no matching telemetry.
- When following an attribute value into SQL, keep the same service, time window, attribute owner and value type so the query finds the same records.

## Discoverability

Your agent needs to be able to find the user manual whether it’s installing the viewer for the first time or using one that’s already running.
So how do we get the right guidance in front of it, whichever workflow it chooses?

By planting the instructions in all the places where agents are likely to trip over them:

1. **Repository README**

   The [repository’s README](https://github.com/CtrlSpice/otel-desktop-viewer) includes installation instructions and a [For agents](https://github.com/CtrlSpice/otel-desktop-viewer#-for-agents) section, so an agent can read the guide before it even installs the viewer or a skill.

2. **CLI help**

   The CLI’s `--help` lists `skills` first; that command prints the investigation instructions.
   This makes the guide available from the executable itself, without a separate skill installation.

   {{< agent-session
     command="otel-desktop-viewer --help"
     output=`COMMANDS
  skills      🧠 Print the agent usage guide
  logs        🪵 Search logs in a running viewer
  log         🪵 Inspect one complete log
  metrics     📈 Search metrics in a running viewer
  metric      📈 Inspect one Metric or its series datapoints
  traces      🧵 Search traces in a running viewer
  trace       🧵 Inspect one complete trace
  span        🧵 Inspect one span
  attributes  🏷️ Discover attribute keys and values
  services    🛎️ Discover services and their telemetry counts
  query       🦆 Run SQL against a running viewer
  export      📤 Export stored telemetry as OTLP JSON
  import      📥 Import OTLP JSON files into a running viewer`
     moreOutput=`USAGE
  otel-desktop-viewer [flags]
  otel-desktop-viewer <command> [flags]

VIEWER FLAGS
      --host string                     Address used by viewer/OTLP receivers (default "localhost")
      --http int                        OTLP HTTP port (default 4318)
      --grpc int                        OTLP gRPC port (default 4317)
      --browser-port int                Viewer HTTP port (default 8000)
      --open-browser                    Open viewer after startup (default true)
      --db string                       DuckDB file; omit for memory
      --db-max-size string              Maximum telemetry-store size (defaults to 512MB in memory or 2GB with --db; 0 disables pruning)
      --self-telemetry-endpoint string  Export the viewer's own traces and metrics to this OTLP/gRPC endpoint

GLOBAL FLAGS
  -h, --help      Help for this command
  -v, --version   Version for otel-desktop-viewer`
   >}}
   {{< /agent-session >}}

3. **Viewer homepage**

   The homepage links to `/llms.txt`, which serves the same guide that `otel-desktop-viewer skills` prints.
   Both match the version of the viewer you’re running.

4. **Installed skill**

   The installable [skill](https://github.com/CtrlSpice/otel-desktop-viewer/tree/main/skills/otel-desktop-viewer) makes the instructions available in the agent’s own skill library.
   Its description helps the agent recognise relevant tasks and load the guide before contacting the viewer.

{{< hire-cta show=true >}}

## Evals

> “Under the most rigorously controlled conditions of pressure, temperature, volume, humidity, and other variables, the organism will do as it damn well pleases.”
>
> — Harvard Law of Animal Behaviour

So how do we know if we got it right?
Can I count on my agent to get the right answer consistently?
And how do smaller models hold up?
That’s what evals are for.

For this post, I gave 11 models the same shop telemetry and five simple tasks, with three attempts at each.
GPT-6.1 Sol graded the answers in fresh conversations without seeing which model wrote them.

Short of some light misconfiguration on my part (sorry! 🇨🇦), everyone did really well.
The three Flash models even passed 41 of their 45 attempts!

{{< screenshot-gallery label="How many customers had a failed checkout?" >}}
{{< screenshot-slide caption="DeepSeek V4.1 Flash" src="/images/viewer-agent-support/deepseek-v4.1-flash.png" width="1680" height="960" alt="DeepSeek counted 32 distinct customer IDs at checkout. Four customers had a failed checkout (12.5%). This attempt passed in 3.3 minutes." >}}
{{< screenshot-slide caption="GLM 5.3 Flash" src="/images/viewer-agent-support/glm-5.3-flash.png" width="1680" height="960" alt="GLM found that checkout ended in an error for 4 of the 32 customers who tried it (12.5%). This attempt passed in 1 minute." >}}
{{< screenshot-slide caption="MiniMax M3.1 Flash" src="/images/viewer-agent-support/minimax-m3.1-flash.png" width="1680" height="960" alt="MiniMax reported a 12.5% failure rate: 4 customers had a failed checkout out of 32 who tried. This attempt passed in 4.4 minutes." >}}
{{< /screenshot-gallery >}}

And then there’s Haiku.
I asked Haiku the same checkout question three times.
Here’s how each attempt went:

1. Listed `./bin`, guessed some HTTP routes, then handed an `http://` URL to the file reader.
2. Grepped the `otel-desktop-viewer` executable for `/api/` instead of running it.
   Found 30 matches and pulled the string it needed (`/openrpc.json`) out of the binary, which is certainly one way to do it...
3. Finally ran `--help`, opened the guide, and got it right: 4 of 32 customers.

{{< screenshot-gallery label="Haiku’s three attempts to count customers with a failed checkout" >}}
{{< screenshot-slide caption="Customer-count attempt" src="/images/viewer-agent-support/haiku-attempt-1.png" width="1680" height="1080" altFile="viewer-agent-support/haiku-attempt-1.txt" >}}
{{< screenshot-slide caption="Customer-count attempt" src="/images/viewer-agent-support/haiku-attempt-2.png" width="1680" height="1080" altFile="viewer-agent-support/haiku-attempt-2.txt" >}}
{{< screenshot-slide caption="Customer-count attempt" src="/images/viewer-agent-support/haiku-attempt-3.png" width="1680" height="1080" altFile="viewer-agent-support/haiku-attempt-3.txt" >}}
{{< /screenshot-gallery >}}

These weird runs give me good information, too.
When I checked the tool calls, I could see that Haiku had the right URL, even if it took the scenic route to get there.
After all that, my test harness rejected its GET because it used the short-form `-m` instead of `--max-time` for the same timeout.
That’s fixed now.

Another example: Luna 6 is the reason we have `/llms.txt`.
It kept going straight to the HTTP endpoint and guessing API routes, so I put the instructions in its way.

## What’s next

So, that Evals tab I’ve been casually showing off?
I’d like to get it out of `[experimental]` and into your hands so you can see what your own agents get up to.

## Try it out

The CLI and agent guide ship with [otel-desktop-viewer v0.8.0](https://github.com/CtrlSpice/otel-desktop-viewer/releases/tag/v0.8.0).
Once you’ve installed it, send it telemetry from your service.
Then give your agent the viewer’s URL and ask it to investigate.
