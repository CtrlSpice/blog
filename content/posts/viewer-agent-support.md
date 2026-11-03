+++
date = '2026-10-13'
draft = true
title = 'Untitled.'
type = 'page'
url = '/viewer-agent-support/'
+++

## Opening

- In the last couple of years, agents have fundamentally changed how we write and observe software. The shape of the software we observe has also changed, and the UI has to change with it.
- There are people better positioned than me to speculate about where this is all going.
What I do know is that some of my new customers are text elementals.
- I want everyone to have a good time, and I want the viewer to stay relevant to how people use software.

## Agent access

> “Here I am, brain the size of a planet and they ask me to take you down to the bridge.”
>
> — Marvin, *The Hitchhiker’s Guide to the Galaxy*, Douglas Adams ([source](https://www.goodreads.com/quotes/175319-come-on-he-droned-i-ve-been-ordered-to-take-you))

- I approached agent access methodically: each access point answers a different question.
  - Help: what commands are available, and how do I invoke them?
  - Skills: how do I use this tool effectively for my task?
  - OpenRPC: how do I call the API directly, and what data does it expect and return?

### Help: what can this tool do?

- An agent arriving at the CLI needs to discover its capabilities.
- Root help lists the commands; each command's help explains its options and gives examples.
  Both work without starting the viewer.

```sh
otel-desktop-viewer --help
otel-desktop-viewer traces --help
```

### Skills: how do I use it well?

- Make the instructions available where the agent encounters the tool.
  The guide has three entry points:
  - **At the terminal:** `otel-desktop-viewer skills` prints the guide embedded in the installed executable.
    An agent with the command available can read its instructions without starting the viewer or fetching them from elsewhere.
  - **Over HTTP:** `/llms.txt` serves the guide from the running viewer.
    An agent accessing the service by URL can get the instructions from that service too.
  - **In the agent's skill library:** `npx skills add CtrlSpice/otel-desktop-viewer --skill otel-desktop-viewer` installs the repository skill into its environment.
    The instructions can be available before the agent first connects to a viewer.
- These are three ways to reach one maintained guide.
  The CLI and HTTP copies are bundled with the viewer, so they match the installed version.

```sh
otel-desktop-viewer skills
```

- Command syntax:
  The skill supplies the workflow: connect to a viewer, choose a command for the question, interpret its results, and follow up.
- It explains operational details that affect the investigation.
  - Reuse an existing viewer, or manage a foreground child process the agent starts itself.
  - Keep using the same viewer for follow-up questions.
  - Choose a focused command, with SQL available for custom questions.
  - Distinguish received telemetry from computed summaries and chart data.

### OpenRPC: how do I talk to the API directly?

- In the evals, some agents tried talking to the API directly even though they knew the CLI was available.
- I added OpenRPC as a contingency: if they're going to take that route, put the API instructions where they can find them.
- OpenRPC describes the viewer's JSON-RPC methods in a machine-readable format: parameters, results, and examples.
  It also covers details such as passing timestamps and 64-bit integers without losing precision.
- The API is at `POST /rpc`; its description is available through `GET /openrpc.json` or `rpc.discover`.
  The HTTP root advertises both `/openrpc.json` and `/llms.txt`, so an agent approaching over HTTP can find the API description and usage guide.
- I'm not actually sure how much this helps yet, but Luna 6 is going to do what it wants.

```sh
curl http://localhost:8000/openrpc.json
```

| Layer | Supporting source |
| --- | --- |
| CLI help | [#570](https://github.com/CtrlSpice/otel-desktop-viewer/pull/570), `root_command.go`. |
| Usage skill | [#567](https://github.com/CtrlSpice/otel-desktop-viewer/pull/567), `skills/otel-desktop-viewer/SKILL.md`. |
| Bundled CLI and HTTP guide | [#580](https://github.com/CtrlSpice/otel-desktop-viewer/pull/580), [#581](https://github.com/CtrlSpice/otel-desktop-viewer/pull/581), `skills/guide.go`. |
| OpenRPC discovery | [Implementation](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.6.0/desktopexporter/internal/server/openrpc.go), [discovery and schema tests](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.6.0/desktopexporter/internal/server/openrpc_test.go), [HTTP/API documentation](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.6.0/docs/ARCHITECTURE.md#http-and-json-rpc). |
| Further tool-design context | [Anthropic's tool-design article](https://www.anthropic.com/engineering/writing-tools-for-agents) discusses clear inputs, outputs, and relevant context. |

- These layers explain how to use the viewer.
  The next question is what telemetry this particular viewer contains.

## Give it enough context to ask the next question

- Discovery helps the agent choose where to look.
  Service summaries show what is present; attribute discovery reveals available keys, types, and common values.
- Compact summaries lead to exact records.
  A service and time window narrow the trace search; trace and span details expose the associated spans and logs.
  Log and metric commands provide the corresponding full details for those signals.
- Correlation has to identify the right record.
  A span is identified by both trace ID and span ID; associated logs must match the exact pair.
- SQL gives the agent room to ask questions beyond the convenience commands.
  Schema inspection is available through the same connection.

```sh
otel-desktop-viewer services
otel-desktop-viewer traces --service checkout --since 30m
otel-desktop-viewer query 'SHOW TABLES'
otel-desktop-viewer query 'DESCRIBE spans'
```

- Readable tables and optional JSON give the agent ways to consume those results.
  Tables are rendered with ordinary Go; DuckDB selects and calculates the results.

| Supporting example | Detail | Source |
| --- | --- | --- |
| Service discovery | `services` provides cross-signal counts and service context. Counts describe stored telemetry, not throughput, service health, or proof a process is alive. | [#588](https://github.com/CtrlSpice/otel-desktop-viewer/pull/588), `services_command.go`. |
| Signal summaries | `traces`, `logs`, and `metrics` accept endpoint, service, time, and result-limit options. They default to the last hour and 25 summaries. | [#568](https://github.com/CtrlSpice/otel-desktop-viewer/pull/568), `telemetry_search.go`. |
| Attribute discovery | `attributes keys` and `attributes values <key>` retain types and owner locations, counts, denominators, relative frequency, and truncation. Find matching record IDs with SQL, then use detail commands. | [#582](https://github.com/CtrlSpice/otel-desktop-viewer/pull/582), [#583](https://github.com/CtrlSpice/otel-desktop-viewer/pull/583), `attributes_command.go`. |
| Trace and span inspection | `trace <trace-id>` returns compact span and trace-linked log rows. `span <span-id>` reports no match, one exact match, or ambiguous matches. `span <trace-id> <span-id>` selects the exact pair and its full correlated logs. | [#576](https://github.com/CtrlSpice/otel-desktop-viewer/pull/576), `trace_command.go`, `span_command.go`. |
| Log and metric details | `log <log-ref>` returns the full typed body and context. `metric <metric-ref>` returns the descriptor, context, and series catalogue; `--series <series-ref>` returns retained datapoints and exemplars. Selected-series detail has optional inclusive time bounds, without an implicit last-hour window or point limit. | [#591](https://github.com/CtrlSpice/otel-desktop-viewer/pull/591), `log_command.go`, `metric_command.go`. |
| Metric identity | Catalogue, detail, and selected-series methods are separate from chart-view methods. Public references are `metricRef` and `seriesRef`; tables are `metrics`, `metric_series`, and `metric_datapoints`. | [#575](https://github.com/CtrlSpice/otel-desktop-viewer/pull/575), [#577](https://github.com/CtrlSpice/otel-desktop-viewer/pull/577), [#578](https://github.com/CtrlSpice/otel-desktop-viewer/pull/578). |
| SQL | `query` uses the running viewer's JSON-RPC API. It accepts one read-only SELECT or supported introspection statement and uses a read-only transaction. The default limit is 25 returned rows; one extra row determines truncation. | [#565](https://github.com/CtrlSpice/otel-desktop-viewer/pull/565), [#566](https://github.com/CtrlSpice/otel-desktop-viewer/pull/566), `query_command.go`. |

- Technical details to retain while drafting:
  - Attribute frequencies count records referencing the attribute: a resource shared by 100 spans contributes 100.
    A histogram datapoint contributes one, regardless of its observation count.
    Events and exemplars use their own timestamps for time filters.
  - Frequency divides eligible records carrying a typed value by eligible records carrying the key, before limiting the displayed values.
    Multiple values on one record can make percentages sum above 100%.
  - Trace-linked logs may have no span ID or no matching stored span.
    A trace overview's log previews are not complete bodies.
  - Viewer-generated log, metric, series, and datapoint references are local to the database.
    Trace and span IDs are received OpenTelemetry identifiers.
  - Received values, calculated summaries, and display formatting are distinct.
    Types, exact integers and timestamps, presence, and owner associations matter.
  - JSON is command-specific: SQL returns columns, rows, and truncation; signal-summary commands return bare arrays without time-window or truncation metadata.
  - Empty searches succeed.
    Span not-found is a structured successful response; missing log or metric detail returns an error.
  - SQL supplies its own time filters.
    Returned-row limits do not bound all query execution costs.
    Read-only prevents database writes; it is not a filesystem or network sandbox.
  - Schema discovery uses `SHOW TABLES` and `DESCRIBE`.
    Release 0.6.0 uses schema 24 and rejects older databases without migrating or modifying them.

- Once it can query and inspect telemetry, the agent can use what it finds to answer a question—or generate a graph to explain the answer.
  That is where access to data meets the design of its presentation.

## Now it can make a graph. What makes that graph useful?

- Agents can generate queries and custom dashboards.
  Useful graphs still depend on decisions about what to measure, what to combine, and what to show.
- These are familiar observability design problems, whoever writes the graph code.
  Two examples from the viewer make the work concrete.
  [Prometheus's dashboard guidance](https://prometheus.io/docs/practices/consoles/) starts with questions and likely failures; it is design guidance rather than an AI comparison study.

### Keep the interesting spike

- A small chart needs fewer points, but the reduction should preserve the event someone is looking for.
- [The sparkline test][arg-peak] supplies 200 readings, normally `1`, with one spike at `500`.
  It checks that reduction produces fewer points while keeping the maximum at `500`.
  The request targets eight sparkline buckets; SQL retains minimum and maximum values at their actual timestamps.
- Reduction chooses which points survive.
  Interpolation is a separate choice about how to draw between them.
  [D3 documents curve behaviour](https://d3js.org/d3-shape/curve), including curves that smooth or overshoot and monotone curves that introduce no intermediate extrema.
  [Aggregate lines use step-after][arg-step] to avoid implying continuous change between window values.
- Visual: the full readings and reduced sparkline, with the same spike visible in both.

### Combine measurements that mean the same thing

- Exponential-histogram buckets can cover different value ranges even when they occupy the same array position.
  Scale and offset determine those ranges.
- [The merge example][arg-exp-merge]: scale 2 / offset 4 counts `[1,1,1,1]` become `[2,2]` at scale 1 / offset 2.
  Adding the second histogram's `[5,5]` then gives `[7,7]`.
  Both histograms cover the same value range; the total count is `14`.
- Value-range alignment, time-window alignment, and exemplar links are different operations in the [OTel metric data model](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram).
- Another possible example: [cumulative readings of `10`, then `30`][arg-cumulative], with a scale change between them, produce interval activity of `20` after alignment.
  The baseline and time window determine what the chart means.
- Visual: bucket ranges before alignment, after alignment, and after addition.

### Make the calculations inspectable

- DuckDB performs the viewer's chart calculations; tests check independently known results, including [output from a real OTel SDK][arg-sdk-test].
  The SDK test records three observations and flushes, then records five more and flushes again through a real OTLP/gRPC exporter.
  The test compares the calculated window count, sum, and buckets with the five independently recorded observations.
- Received datapoints remain available separately from calculated rates, quantiles, and reduced chart data.
  Someone can inspect what was recorded as well as what the graph displays.
- Supporting implementation: [metric resolution](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.6.0/docs/metric-resolution.md).
  The linked chart fixtures were inspected during research, not rerun, and some predate agent support.

| Additional code reference at `f77ec6d9` | Evidence |
| --- | --- |
| `desktopexporter/internal/store/queries/metrics/get_metric_view.sql:488–529` | Sparkline min/max selection retains original timestamps. |
| Same file, lines 533–574 | Main scalar reduction elects first/last/min/max points. |
| Same file, lines 579–622 | Exponential-histogram downscaling and offset alignment. |
| `desktopexporter/internal/store/queries/downscale_test.go:19–125` | Hand-worked downscaling and count-conservation checks. |
| `desktopexporter/internal/store/metrics/metrics_test.go:5252–5315` | Time-bucket alignment across timezone offsets. |

- Preserving peaks and aligning histogram buckets affect what someone can see and understand from the data.
  The next part is giving them ways to explore it.

## Give people something they can explore

- I feel like it isn't so much debugging as furthering your own understanding that will lead you to use your visual cortex for things like observability.
- Staying relevant to people also means adapting to the shape of the telemetry they're trying to understand.
  - Deeply nested attributes are a concrete example: displaying all the values isn't enough to make their structure readable.
  - I want people to be able to follow that structure, find the part they care about, and understand where it belongs.
  - This is the UI's job here: make the data readable so people can use it to further their understanding.
- The human interface has a purpose beyond checking an agent's answer: helping people develop their own understanding of the system.
  Agents can query and inspect telemetry; people can explore relationships, timing, and distributions visually.
- A good user experience is still the defining feature of the product.
  People should be able to follow their own questions.
  Possible aside: “may cause strong cravings for taffy, apply taffy to face hole as needed”.
- Discovery gives people starting points too.
  Home shows services, recorded connections, and matching telemetry.
- Following a connection should preserve the question's context.
  Home drill-through keeps the search and exact time window; metric exemplars link measurements to their traces and spans.
- Small interaction choices make that exploration comfortable.
  Selecting a visible span keeps it in place; detail sections stay stable between selections.
  Inspecting a metric series is independent of changing which series the chart displays.
- Individual telemetry exports let someone reopen the evidence in another viewer.
  Trace exports contain retained spans; correlated logs are exported separately.
- Visual: a chart-to-exemplar-to-span sequence showing how a person continues an investigation.

| Supporting example | Detail and source |
| --- | --- |
| Visual discovery | Home's services, service map, telemetry cards, and attribute discovery provide ways to explore. [Explore Telemetry](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.6.0/README.md#explore-telemetry). |
| Exact drill-through window | [b59427d4](https://github.com/CtrlSpice/otel-desktop-viewer/commit/b59427d4). |
| Stationary span selection | [5024d7a6](https://github.com/CtrlSpice/otel-desktop-viewer/commit/5024d7a6). |
| Stable details and source-sample selection | [7c143898](https://github.com/CtrlSpice/otel-desktop-viewer/commit/7c143898). |
| Independent series inspection and chart visibility | [54e6ecfa](https://github.com/CtrlSpice/otel-desktop-viewer/commit/54e6ecfa). |
| Exemplar navigation | [The UI][arg-exemplar-ui] shows the received value and timestamp and links to the trace/span. [Tests][arg-exemplar-test] check the links and removal of metric-specific navigation state. |
| Export and import | `export trace`, `export log`, and `export metric` have corresponding HTTP downloads and UI actions. Home's file picker and global drop accept OTLP JSON/JSONL. [#587](https://github.com/CtrlSpice/otel-desktop-viewer/pull/587), [#592](https://github.com/CtrlSpice/otel-desktop-viewer/pull/592), [export/import documentation](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.6.0/docs/otlp-json-export.md). |

- Sharing details to retain while drafting:
  - Exports reconstruct retained telemetry, including its resource/scope context, rather than recovering original request bytes.
    Search filters and collapsed rows do not narrow the exported entity.
  - A trace export includes retained orphaned or cyclic spans, but not linked spans from other traces or correlated logs.
  - Preserve export bytes: parsing and re-encoding JSON in JavaScript can lose negative zero.
    Exports cannot recover rejected or unsupported data, original request segmentation, or every encoding distinction.
  - Browser import sends original payload slices sequentially through the OTLP HTTP receiver in requests of at most 20 MiB.
    The receiver must be reachable from the browser.
    Ingestion issues report problems; supported groups and subsequent files can continue after individual failures.
  - Imported payloads pass through the Collector's redaction processor before storage.
    Matching secret values are masked.
  - Receiver acceptance precedes completed database writes.
  - The described 0.6.0 import workflow is browser import; individual exports use JSON.

## A good time for everyone

- Agent access and the human UI support the same aim: understanding telemetry and being able to explore further.
  Instructions, useful context, deliberate calculations, and comfortable interaction all contribute to that experience.
- These are concrete ways to adapt the tool to how people now use software.
- The features described here are available in [0.6.0](https://github.com/CtrlSpice/otel-desktop-viewer/releases/tag/v0.6.0).
  The [usage guide](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.6.0/skills/otel-desktop-viewer/SKILL.md) is a starting point for trying the CLI.
- Next up is evals in 0.7.0, with its own launch post.

[arg-peak]: https://github.com/CtrlSpice/otel-desktop-viewer/blob/f77ec6d92618dd8ce38cdee392aaa6387bc28b23/desktopexporter/internal/store/metrics/metrics_test.go#L4491-L4566
[arg-exp-merge]: https://github.com/CtrlSpice/otel-desktop-viewer/blob/f77ec6d92618dd8ce38cdee392aaa6387bc28b23/desktopexporter/internal/store/metrics/metrics_test.go#L4584-L4661
[arg-cumulative]: https://github.com/CtrlSpice/otel-desktop-viewer/blob/f77ec6d92618dd8ce38cdee392aaa6387bc28b23/desktopexporter/internal/store/metrics/metrics_test.go#L4663-L4725
[arg-sdk-test]: https://github.com/CtrlSpice/otel-desktop-viewer/blob/f77ec6d92618dd8ce38cdee392aaa6387bc28b23/desktopexporter/internal/store/metrics/sdk_cumulative_test.go#L40-L145
[arg-step]: https://github.com/CtrlSpice/otel-desktop-viewer/blob/f77ec6d92618dd8ce38cdee392aaa6387bc28b23/desktopexporter/internal/frontend/src/components/metrics/Charts/MetricTimeSeriesChart.svelte#L180-L200
[arg-exemplar-ui]: https://github.com/CtrlSpice/otel-desktop-viewer/blob/f77ec6d92618dd8ce38cdee392aaa6387bc28b23/desktopexporter/internal/frontend/src/components/metrics/Detail/SeriesDatapointList.svelte#L382-L408
[arg-exemplar-test]: https://github.com/CtrlSpice/otel-desktop-viewer/blob/f77ec6d92618dd8ce38cdee392aaa6387bc28b23/desktopexporter/internal/frontend/src/components/metrics/Detail/SeriesDatapointList.test.ts#L562-L617
