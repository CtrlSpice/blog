+++
date = '2026-10-13'
draft = true
title = 'Untitled.'
type = 'page'
url = '/viewer-agent-support/'
+++

## Opening

- In the last couple of years, agents have fundamentally changed how we write and observe software.
The shape of the software we observe has also changed, and the UI has to change with it.
- I'll let people better positioned than me speculate about where this is all going.
I've got new customers, and some of them are text elementals.
- I want everyone to have a good time, and I want the viewer to stay relevant to how people use software.

## Agent access

> “Here I am, brain the size of a planet and they ask me to take you down to the bridge.”
>
> — Marvin, *The Hitchhiker’s Guide to the Galaxy*, Douglas Adams ([source](https://www.goodreads.com/quotes/175319-come-on-he-droned-i-ve-been-ordered-to-take-you))

- I approached agent access through three questions:
  - Help: what commands are available, and how do I invoke them?
  - Skills: how do I use this tool effectively for my task?
  - OpenRPC: how do I call the API directly, and what data does it expect and return?

### Help: what can this tool do?

- Root help lists the commands.
  Each command's help explains its options and gives examples.
- An agent can find out what the tool does without starting the viewer.

```sh
otel-desktop-viewer --help
otel-desktop-viewer traces --help
```

### Skills: how do I use it well?

- Make the instructions available where the agent encounters the tool.
  The guide has three entry points:
  - **At the terminal:** `otel-desktop-viewer skills` prints the guide embedded in the installed executable.
    The instructions are there even when the viewer isn't running or the agent can't fetch them from elsewhere.
  - **Over HTTP:** `/llms.txt` serves the guide from the running viewer.
    An agent with the viewer's URL can get its instructions from the same place.
  - **In the agent's skill library:** `npx skills add CtrlSpice/otel-desktop-viewer --skill otel-desktop-viewer` installs the repository skill into its environment.
    It can have the instructions before it first connects to a viewer.
- I maintain one guide.
  The CLI and HTTP versions ship with the viewer, so they match the installed version.

```sh
otel-desktop-viewer skills
```

- Command syntax:
  The skill explains how to connect, pick a command for the question, read the result, and follow up.
  It also covers how to work with a running viewer:
  - Reuse an existing viewer, or manage a foreground child process the agent starts itself.
  - Keep using the same viewer for follow-up questions.
  - Use a focused command when it answers the question, and SQL when it doesn't.
  - Keep track of which values arrived as telemetry and which the viewer calculated.

### OpenRPC: how do I talk to the API directly?

- In the evals, some agents tried talking to the API directly even though they knew the CLI was available.
- I added OpenRPC as a contingency.
  If they're going to take that route, the API instructions should be in their path.
- OpenRPC gives them a machine-readable description of the JSON-RPC methods, including parameters, results, and examples.
  It explains how to pass timestamps and 64-bit integers without losing precision.
- Requests go to `POST /rpc`.
  The description is available at `GET /openrpc.json` and through `rpc.discover`.
  The HTTP root advertises both `/openrpc.json` and `/llms.txt`, so an agent can discover the API description and usage guide together.
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

## What telemetry is here?

- `services` shows which services have sent telemetry.
  Attribute discovery shows the keys, types, and common values in this store.
- The agent can narrow a trace search to a service and time window, then follow a result to its spans and logs.
  Log and metric commands provide full details for those signals too.
- A span ID on its own can be ambiguous.
  The trace ID and span ID identify the exact span, and its associated logs must match both.
- When the commands don't answer the question, the agent can write SQL.
  It can inspect the schema through the same connection.

```sh
otel-desktop-viewer services
otel-desktop-viewer traces --service checkout --since 30m
otel-desktop-viewer query 'SHOW TABLES'
otel-desktop-viewer query 'DESCRIBE spans'
```

- Tables provide a readable overview, with `--json` available for structured results.
  Go handles the table layout, and DuckDB selects and calculates the results.

| Supporting example | Detail | Source |
| --- | --- | --- |
| Service discovery | `services` provides cross-signal counts and service context. Counts describe stored telemetry, not throughput, service health, or proof a process is alive. | [#588](https://github.com/CtrlSpice/otel-desktop-viewer/pull/588), `services_command.go`. |
| Signal summaries | `traces`, `logs`, and `metrics` accept endpoint, service, time, and result-limit options. They default to the last hour and 25 summaries. | [#568](https://github.com/CtrlSpice/otel-desktop-viewer/pull/568), `telemetry_search.go`. |
| Attribute discovery | `attributes keys` and `attributes values <key>` retain types and owner locations, counts, denominators, relative frequency, and truncation. Find matching record IDs with SQL, then use detail commands. | [#582](https://github.com/CtrlSpice/otel-desktop-viewer/pull/582), [#583](https://github.com/CtrlSpice/otel-desktop-viewer/pull/583), `attributes_command.go`. |
| Trace and span inspection | `trace <trace-id>` returns compact span and trace-linked log rows. `span <span-id>` reports no match, one exact match, or ambiguous matches. `span <trace-id> <span-id>` selects the exact pair and its full correlated logs. | [#576](https://github.com/CtrlSpice/otel-desktop-viewer/pull/576), `trace_command.go`, `span_command.go`. |
| Log and metric details | `log <log-ref>` returns the full typed body and context. `metric <metric-ref>` returns the descriptor, context, and series catalogue. `--series <series-ref>` returns retained datapoints and exemplars. Selected-series detail has optional inclusive time bounds, without an implicit last-hour window or point limit. | [#591](https://github.com/CtrlSpice/otel-desktop-viewer/pull/591), `log_command.go`, `metric_command.go`. |
| Metric identity | Catalogue, detail, and selected-series methods are separate from chart-view methods. Public references are `metricRef` and `seriesRef`. Tables are `metrics`, `metric_series`, and `metric_datapoints`. | [#575](https://github.com/CtrlSpice/otel-desktop-viewer/pull/575), [#577](https://github.com/CtrlSpice/otel-desktop-viewer/pull/577), [#578](https://github.com/CtrlSpice/otel-desktop-viewer/pull/578). |
| SQL | `query` uses the running viewer's JSON-RPC API. It accepts one read-only SELECT or supported introspection statement and uses a read-only transaction. The default limit is 25 returned rows. One extra row determines truncation. | [#565](https://github.com/CtrlSpice/otel-desktop-viewer/pull/565), [#566](https://github.com/CtrlSpice/otel-desktop-viewer/pull/566), `query_command.go`. |

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
  - JSON is command-specific.
    SQL returns columns, rows, and truncation.
    Signal-summary commands return bare arrays without time-window or truncation metadata.
  - Empty searches succeed.
    Span not-found is a structured successful response.
    Missing log or metric detail returns an error.
  - SQL supplies its own time filters.
    Returned-row limits do not bound all query execution costs.
    Read-only prevents database writes.
    It is not a filesystem or network sandbox.
  - Schema discovery uses `SHOW TABLES` and `DESCRIBE`.
    Release 0.6.0 uses schema 24 and rejects older databases without migrating or modifying them.

- An agent can use those queries to build a dashboard or explain an answer with a graph.

## What makes the graph useful?

- A graph needs to answer a question.
  Someone still has to choose what to measure, what to combine, and what to show, whoever writes the code.
- Peak preservation and histogram merging are two examples from the viewer of the work behind that presentation.
  [Prometheus's dashboard guidance](https://prometheus.io/docs/practices/consoles/) also starts with questions and likely failures.
  It is design guidance, not an AI comparison study.

### Keep the interesting spike

- [The sparkline test][arg-peak] supplies 200 readings, normally `1`, with one spike at `500`.
  It requests eight target buckets and checks that fewer points remain while the maximum is still `500`.
- A sparkline has less room than the full chart, but that spike still matters.
  SQL keeps the minimum and maximum values at their actual timestamps.
- Choosing which points survive is data reduction.
  Choosing how to draw between them is interpolation.
  [D3 documents curve behaviour](https://d3js.org/d3-shape/curve), including curves that smooth or overshoot and monotone curves that introduce no intermediate extrema.
  [Aggregate lines use step-after][arg-step] to avoid implying continuous change between window values.
- Visual: the full readings and reduced sparkline, with the same spike visible in both.

### Line up the histogram buckets

- Exponential-histogram buckets can cover different value ranges even when they occupy the same array position.
  Scale and offset determine those ranges.
- [The merge example][arg-exp-merge]: scale 2 / offset 4 counts `[1,1,1,1]` become `[2,2]` at scale 1 / offset 2.
  Adding the second histogram's `[5,5]` then gives `[7,7]`.
  Both histograms cover the same value range, and the total count is `14`.
- Value-range alignment, time-window alignment, and exemplar links are different operations in the [OTel metric data model](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram).
- Another possible example: [cumulative readings of `10`, then `30`][arg-cumulative], with a scale change between them, produce interval activity of `20` after alignment.
  The baseline and time window determine what the chart means.
- Visual: bucket ranges before alignment, after alignment, and after addition.

### Make the calculations inspectable

- [The SDK test][arg-sdk-test] records three observations and flushes, then records five more and flushes again through a real OTLP/gRPC exporter.
  It checks that the calculated window count, sum, and buckets match the five observations recorded between flushes.
- DuckDB does the chart calculations.
  Hand-worked fixtures and the SDK test give us independent expectations to check them against.
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

- The graph can also give someone a starting point for exploring the system themselves.

## Give people something they can explore

- I feel like it isn't so much debugging as furthering your own understanding that will lead you to use your visual cortex for things like observability.
  The visual interface is a way to explore relationships, timing, and distributions, beyond checking an agent's answer.
- Deeply nested attributes are part of the changing shape of that telemetry.
  Showing every value doesn't necessarily make the structure readable.
  I want people to be able to find the part they care about and understand where it belongs.
- A good user experience is still the defining feature of the product.
  People should be able to follow their own questions.
  Possible aside: “may cause strong cravings for taffy, apply taffy to face hole as needed”.
- Home shows services, their recorded connections, and matching telemetry.
  Opening a result keeps the search and exact time window, so the next view still covers the data you were looking at.
- A metric exemplar links a measurement to its trace and span.
  You can follow the number into the recorded activity behind it.
- Selecting a visible span keeps it in place, and detail sections stay stable between selections.
  Comparing records shouldn't mean finding your place again each time.
- Inspecting a metric series doesn't change which series the chart displays.
  You can examine one series while keeping the comparison on screen.
- Individual telemetry exports can be opened in another viewer.
  A trace export contains its retained spans, with correlated logs exported separately.
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
    Ingestion issues report problems.
    Supported groups and subsequent files can continue after individual failures.
  - Imported payloads pass through the Collector's redaction processor before storage.
    Matching secret values are masked.
  - Receiver acceptance precedes completed database writes.
  - The described 0.6.0 import workflow is browser import.
    Individual exports use JSON.

## Next: evals

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
