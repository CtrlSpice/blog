+++
date = '2026-10-13'
draft = true
title = 'Untitled'
+++

- Explain what changed since you started building the viewer:
  - fundamental premise of software may have shifted a wee bit in the last couple of years
    - “ideally before you have to pay your back-end provider for the cardinality blowout or whatever”
- Show a specific mistake that could matter:
  - A request ID used as a metric attribute.
  - An HTTP response marked as an error without enough context.

| Claim | Assessment | Evidence and wording |
| --- | --- | --- |
| “o11y has changed” with AI | Use your experience as the opening. | Describe the changes in your work and what the product needs to do. The new command-line interface (CLI) and guide show your response. They do not measure change across the industry. |
| “instrumentation is easy AF” | Too broad as a general claim. | Agents can help write SDK setup, exporters, and spans. Deciding what to measure and checking the result are separate tasks. Describe the ease as your experience if that is the basis. OTel's [zero-code documentation][arg-zero-code] distinguishes automatic library instrumentation from instrumentation of application code. |
| “they aren't super good at nuance” | Give a specific example. | OTel's [HTTP conventions][arg-http-status] treat 4xx responses differently for client and server spans. Application context can also affect the classification. These rules show why context matters. A personal example or the later evals could show an agent missing that context. |
| Check it before the “cardinality blowout” bill | Supported for some metric billing models. | [Datadog documents billing by distinct metric-name and tag combinations][arg-billing]. It also offers other pricing contracts. OTel defines [cardinality as unique attribute combinations][arg-cardinality] and provides limits. A request ID on a metric could increase cardinality. The same ID on a trace or log has different implications. |

| Example | Detail | Point to make |
| --- | --- | --- |
| HTTP status | Without extra context, instrumentation must leave span status unset for SERVER 4xx responses. For CLIENT 4xx responses, it should set Error. An expected 404 existence check provides context for a more precise classification. | Writing a span is straightforward. Deciding what it means requires context. This example comes from the specification, not an observed agent failure. |
| Metric cardinality | A unique request ID used as a metric attribute can create a series for each observed combination. Compare it with a route template such as `/users/{id}`. HTTP conventions require `http.route` to have low cardinality. | Check metric attributes before broad deployment. Local data cannot establish production cardinality or guarantee a particular bill. |

- State the task that the rest of the post addresses:
  1. Exercise the application.
  2. Inspect the telemetry that arrived.
  3. Check whether it shows what you intended to measure.

## 2. Agents need to use the viewer too

- Explain why this changes the viewer's users:
  - “I don't really think observability tools are going away.”
  - “I think we are just supporting a new type of user that is autistic like I am autistic.”
    - Explain which interface needs you share with this new user.
- People already use the viewer to inspect local telemetry.
- Agents need access to that inspection too.
- “in addition to providing y'all with a decent UI (may cause strong cravings for taffy, apply taffy to face hole as needed) I now need to make the thing navigable by robits [sic]”
- “Yay responsibilities.”

| Claim | Assessment | Evidence and wording |
| --- | --- | --- |
| Observability tools have a new user | A reasonable argument about the future. | Tools still need to store, query, check, and visualise telemetry when an agent starts the investigation. This does not establish which vendors or interfaces will survive. Explain what you intend to keep building. |
| The new user is “autistic like I am autistic” | Explain the personal analogy. | If you mean precise instructions, explicit context, or predictable interfaces, describe those needs in your own terms. The sources do not establish that LLMs have autistic cognition. |
| Make the viewer “navigable by robits” | The implementation supports this. | Help, discovery commands, detail commands, SQL, and the shared guide give agents access. [Anthropic's tool-design article][arg-agent-tools] recommends clear inputs and outputs, relevant context, and limits on results. It does not establish the best output format for this viewer. |

| Feature | Purpose | What landed | Sources |
| --- | --- | --- | --- |
| Local debugging for agents | The August post proposed agent access to queries and evidence that people could inspect. An agent would change instrumentation, exercise the app, inspect telemetry, and correct problems. | The CLI and guide provide inspection tools. A dedicated workflow for agents to review their instrumentation remains future work. | [Previous post, “Coming up”](/rebuilding-otel-desktop-viewer/#coming-up). Current usage skill. |

- Next question: what does an agent need to investigate?

## 3. Give agents a way to investigate

- Follow the questions in an investigation.
- Introduce each command when it helps answer the next question.

### How do I use this thing?

- Show how an agent finds the help and usage guide.
- Explain that `skills` and `/llms.txt` provide the same instructions.

| Feature | Purpose | What landed | Sources |
| --- | --- | --- | --- |
| Command help | List commands, flags, defaults, and examples without starting the viewer. | The root command and each subcommand provide help, descriptive names, and selected emojis. The bare command still runs the viewer in the foreground. Client commands connect to its HTTP endpoint. | [#570](https://github.com/CtrlSpice/otel-desktop-viewer/pull/570). `root_command.go`. |
| Usage skill | Teach agents to operate the viewer before adding framework-specific setup or audit instructions. | `skills/otel-desktop-viewer/SKILL.md` explains connections, process ownership, commands, schema inspection, examples, and results. Install it with `npx skills add CtrlSpice/otel-desktop-viewer --skill otel-desktop-viewer`. | [#567](https://github.com/CtrlSpice/otel-desktop-viewer/pull/567). README and bundled skill. |
| Bundled instructions | Provide a guide that matches the installed viewer version. | `otel-desktop-viewer skills` prints the embedded guide offline. `/llms.txt` serves the same guide through the running viewer. | [#580](https://github.com/CtrlSpice/otel-desktop-viewer/pull/580), [#581](https://github.com/CtrlSpice/otel-desktop-viewer/pull/581). `skills/guide.go`. |

### What data is here, and where should I look?

- Introduce service discovery, signal summaries, and attribute discovery.
- Explain why the first response contains enough information to choose the next question.

| Feature | Purpose | What landed | Sources |
| --- | --- | --- | --- |
| Service discovery | Add a convenience command, with a possible service homepage later. | `services` groups all three signals by namespace and name. It returns span and error-span counts, log and error-log counts, Metric and datapoint counts, and the latest qualifying timestamp. It uses the SQL query API. The proposed homepage remains future work. | [#588](https://github.com/CtrlSpice/otel-desktop-viewer/pull/588). `services_command.go`. |
| Signal summaries | Use existing searches to find telemetry before requesting full records. | `traces`, `logs`, and `metrics` accept endpoint, service, time, and result-limit options. They default to the last hour and 25 rows. They return tables or JSON summaries, including previews and computed fields. | [#568](https://github.com/CtrlSpice/otel-desktop-viewer/pull/568). `telemetry_search.go`. |
| Attribute keys and common values | Find keys, types, and common values without requesting every value. SQL examples came first. | `attributes keys` and `attributes values <key>` support attributes across signals and owners. Results keep types, locations, exact counts, the denominator, and computed relative frequency. They report whether more results exist. Tables show value, kind, count, and percentage without bars. | [#582](https://github.com/CtrlSpice/otel-desktop-viewer/pull/582), [#583](https://github.com/CtrlSpice/otel-desktop-viewer/pull/583). |
| Find records with an attribute | Follow a key and typed value to the records that carry it. | Discover the value, use `query` to find matching IDs, then inspect those records. Attributes keep their resource, scope, event, link, datapoint, exemplar, or metric-metadata associations. | Bundled skill. `attributes_command.go`. |

- Details to preserve:
  - Attribute frequencies count records that reference the attribute. A resource shared by 100 spans contributes 100.
  - A histogram datapoint counts once.
  - Events and exemplars use their own timestamps for time filters.
  - Service counts describe stored telemetry, not request throughput or service health.
  - `traces`, `logs`, and `metrics --json` return bare arrays. They omit the time window and whether more results exist.
  - Do not imply that all commands return the same JSON structure.

### What actually happened in this record?

- Follow a summary to its trace, span, log, or metric details.
- Explain why the answer needs exact types, timestamps, and record associations.

| Feature | Purpose | What landed | Sources |
| --- | --- | --- | --- |
| Trace and span inspection | Inspect a trace and its logs, then one span. Handle span IDs that occur in multiple traces. | `trace <trace-id>` returns every compact span row and trace-linked log row. `span <span-id>` returns not-found, one exact result, or a limited list of ambiguous matches. `span <trace-id> <span-id>` selects the exact pair and its full correlated logs. | [#576](https://github.com/CtrlSpice/otel-desktop-viewer/pull/576). `trace_command.go`, `span_command.go`. |
| Metric identity and datapoints | Identify each Metric and series. Keep received datapoints separate from reduced chart data. | Catalogue, detail, and selected-series methods are separate from chart-view methods. Public references use `metricRef` and `seriesRef`. Tables are `metrics`, `metric_series`, and `metric_datapoints`. This replaced the earlier report-owner design and `metric_ingests` table. | [#575](https://github.com/CtrlSpice/otel-desktop-viewer/pull/575), [#577](https://github.com/CtrlSpice/otel-desktop-viewer/pull/577), [#578](https://github.com/CtrlSpice/otel-desktop-viewer/pull/578). |
| Full log and Metric details | Let agents inspect the records they find through summaries. | `log <log-ref>` returns the complete typed body and context. `metric <metric-ref>` returns its descriptor, context, and series catalogue. `--series <series-ref>` returns retained datapoints and exemplars. Optional time bounds are inclusive. Selected-series detail has no implicit last-hour window or point limit. | [#591](https://github.com/CtrlSpice/otel-desktop-viewer/pull/591). `log_command.go`, `metric_command.go`. |

- Details to preserve:
  - Trace-linked logs can lack a span ID or matching stored span.
  - Exact span correlation requires both the trace ID and span ID.
  - Viewer-generated references are local to the database. Trace and span IDs are received OTel identifiers.
  - Preserve the distinction between received values, computed summaries, and display formatting.

### What if the built-in commands don't answer my question?

- Show how to inspect the schema through the running viewer.
- Use SQL for a question that needs a custom query.
- Explain when to use table output or JSON.

| Feature | Purpose | What landed | Sources |
| --- | --- | --- | --- |
| SQL access | Ask questions beyond predefined UI searches using the existing DuckDB store. | `query '<sql>'` calls the running viewer's JSON-RPC `query` method. It accepts one read-only SELECT or supported introspection statement and uses a read-only transaction. The default result limit is 25 rows. `--limit` changes it. One extra row establishes whether more results exist. The result contains ordered column names and types, row arrays, and `truncated`. | [#565](https://github.com/CtrlSpice/otel-desktop-viewer/pull/565), [#566](https://github.com/CtrlSpice/otel-desktop-viewer/pull/566). |
| Schema discovery | The original plan included dedicated `tables` and `schema` commands. | Use `query 'SHOW TABLES'` and `query 'DESCRIBE spans'`. A later proposal reconsidered a schema command, then dropped it. Current guide examples use schema 22. | Landed skill and query implementation. |
| Tables and exact JSON | The initial plan used JSON first. Review led to compact tables with optional JSON. | Ordinary Go renders aligned columns with underlined headers. `--json` returns structured results. Detailed records use labelled sections and tables. DuckDB selects and computes the results. | [#566](https://github.com/CtrlSpice/otel-desktop-viewer/pull/566). `query_command.go` and detail commands. |

- Details to preserve:
  - The running viewer owns the database, including in-memory mode. CLI commands use its API.
  - Each SQL query supplies its own time filters.
  - A limit on returned rows does not limit all query execution costs.
  - Read-only prevents database writes. It does not make the query a filesystem or network sandbox.

### How can someone else inspect what I found?

- Show how to export retained telemetry.
- Load that export into another viewer.
- Return to the opening question: another person can inspect the evidence from the instrumentation check.

| Feature | Purpose | What landed | Sources |
| --- | --- | --- | --- |
| Export telemetry | Let people and agents save telemetry that the store can already reconstruct. | `export trace`, `export log`, and `export metric` export the complete retained entity. HTTP downloads and UI buttons provide the same access. Individual file exports now use JSON only. | Store getters #542–544. [#587](https://github.com/CtrlSpice/otel-desktop-viewer/pull/587), [#592](https://github.com/CtrlSpice/otel-desktop-viewer/pull/592). |
| Import telemetry files | Complete the save, share, and reload workflow proposed in the August post. | Choose files on Home or drop them anywhere. Browser import accepts JSON, JSONL, and mixed supported signals. It sends original payload slices sequentially through the OTLP HTTP receiver. Each request contains at most 20 MiB. Ingestion issues lists problems. CLI import remains open. | [#592](https://github.com/CtrlSpice/otel-desktop-viewer/pull/592). Open [#597](https://github.com/CtrlSpice/otel-desktop-viewer/pull/597). `docs/otlp-json-export.md`. |

- Details to preserve:
  - Export reconstructs retained telemetry, not original request bytes. Export correlated logs separately from a trace.
  - The receiver can accept data before the database finishes writing it.
  - Recheck CLI import before publication.
  - The status check used main `f77ec6d9` on October 13. The latest published release was still v0.5.0.

## 4. Check what the chart means

- The agent can now read data and generate a graph.
- Explain the choices that affect what the graph means.
- “because yeah the agents can build custom dashboards etc, but that won't suddenly make dashboards more useful”
  - Consider “won't automatically”. Usefulness depends on the question, the calculations, and what the person can inspect.
- “you don't want to have to thing about”
  - “if it got the times lined up correctly for an exphist correlation”
  - “or if the interpolation it is using on a graph is smooshing a peak or somethig”
- Use two examples from the viewer:
  - The reduced sparkline must keep the spike at 500. Separate data reduction from drawing between points.
  - Align exponential-histogram buckets before merging them. Explain how value-range alignment differs from time alignment.
- “I feel like there still have to be solid components backed by blah blah”
  - Replace “blah blah” with the choices and tests in these examples.

| Claim | Assessment | Evidence and wording |
| --- | --- | --- |
| Generated dashboards “won't suddenly make dashboards more useful” | Avoid making this absolute. | A dashboard built for a specific task can be more useful. Data selection, calculations, and interaction still need design. [Prometheus's dashboard guidance][arg-dashboards] starts with questions and likely failures. It is historical design guidance, not an AI comparison study. |
| Times lined up for an “exphist correlation” | Specify the operation. | Time-window alignment, histogram value-range alignment, and exemplar links are different operations. The [OTel data model][arg-metric-model] defines them. The rescaling example below concerns value ranges. |
| Interpolation “smooshing a peak” | Separate reduction from interpolation. | Downsampling can average away minimum and maximum values. Interpolation draws between retained points. Some curves smooth or overshoot. [D3 documents these differences][arg-curves]. Monotone curves do not introduce new minimum or maximum values between adjacent points. |
| “solid components backed by blah blah” | The code provides specific support. | Documented behaviour, explicit calculation and display choices, hand-calculated examples, real SDK tests, and underlying detail support the components. Reuse reduces repeated checking work. It cannot make an incorrect query or conclusion correct automatically. |

| Example | Detail | Point to make |
| --- | --- | --- |
| Preserve a peak | [The test][arg-peak] supplies 200 readings, normally `1`, with one spike at `500`. It requests eight target sparkline buckets. It checks that fewer points remain and the maximum is still `500`. SQL keeps the minimum and maximum values at their actual timestamps. | A small chart needs a deliberate rule for reducing data. Separately, [aggregate lines use step-after][arg-step] so they do not imply continuous change between window values. |
| Merge exponential histograms | [The fixture][arg-exp-merge] combines counts `[1,1,1,1]` at scale 2, offset 4, with counts `[5,5]` at scale 1, offset 2. Both cover the same value range. Downscaling the first gives `[2,2]`. Adding the aligned counts gives `[7,7]`, with a total of `14`. | Adding array positions without aligning them can combine different value ranges. Scale and offset determine each bucket's meaning. |
| Interpret cumulative counts | [The fixture][arg-cumulative] contains cumulative counts of `10`, then `30`, with a scale change between them. After alignment, the expected interval activity is `20`. | A plausible chart can show a running total instead of interval activity. Explain the baseline and time window. |
| Check against SDK output | [The test][arg-sdk-test] records three observations and flushes them. It records five more and flushes again. A real Go SDK OTLP/gRPC exporter sends the data to a test receiver. The test ingests those batches. It checks the window count, sum, and buckets against the five independently recorded observations. | This gives “backed by tests” a specific meaning: compare SDK output with independently calculated expectations. |

- Research checked these existing code and test examples without rerunning them.
- Some examples predate agent support.
- Next question: can an agent use these tools and calculations to complete an investigation?

## 5. Test whether agents can complete an investigation

- Evals: develop this section after the ongoing work is ready.
- Return to the opening question: can the agent inspect evidence and reach an answer that the evidence supports?
- Use the completed results to explain where the interface helped and where it fell short.
- Keep claims within what the results support.
- A command's existence does not establish that agents can use it successfully.

| Supporting feature | Purpose | What landed | Sources |
| --- | --- | --- | --- |
| Reusable telemetry datasets | Capture data once and replay the same files for development and checks. | Fixed OTLP datasets replaced the Perl seed scripts. Loading preserves their bytes. A later change preserves LF line endings on Windows. The checked main contains `demo`, `checkout`, and `usability-pilot`. Add later dataset and eval work when ready. | [#585](https://github.com/CtrlSpice/otel-desktop-viewer/pull/585), [#586](https://github.com/CtrlSpice/otel-desktop-viewer/pull/586). `testdata/otlp/README.md`. |
| Observe the viewer | Inspect the viewer without feeding its own telemetry into its store. | `--self-telemetry-endpoint` optionally exports viewer traces and metrics to an external OTLP/gRPC endpoint, such as another viewer. It is off by default. This could support an implementation anecdote, but does not establish an eval result. | [#574](https://github.com/CtrlSpice/otel-desktop-viewer/pull/574). |

## 6. Keep the evidence useful to people

- “a good user experience still being the defining feature of the product”
- “and you still want the agent to present somethig that you can interact with and explore”
  - Follow a metric exemplar to its trace and span.
  - Show how the person can continue the investigation.

| Claim | Assessment | Evidence and wording |
| --- | --- | --- |
| Something you can “interact with and explore” | An existing capability that you want to preserve. | [Exemplar links][arg-exemplar-ui] connect a datapoint to received trace and span IDs. [Tests][arg-exemplar-test] check those links and that navigation drops metric-specific view state. Explain why you value this experience without claiming every user prefers it. |

| Example | Detail | Point to make |
| --- | --- | --- |
| Follow the evidence | A datapoint's exemplar shows its value and time, with links to its trace and span. UI tests check the URLs and navigation. | The person can inspect the result, follow the evidence, and ask another question. |

- Return to the new user: an agent can start the investigation.
- The product still needs to make the evidence useful to the person receiving it.
- “and the cli is the start.”
- “I want to give it components to play with and see what it can do on its own”
  - End with the experiment: let agents combine components whose behaviour you already understand and test.

| Claim | Assessment | Evidence and wording |
| --- | --- | --- |
| Give agents components | A possible next step with an existing example. | [A2UI catalogs][arg-components] let agents combine application-provided components with defined properties. Examples include interactive charts. This supports the idea, not a choice to use A2UI. The viewer does not yet expose this capability. |

[arg-zero-code]: https://opentelemetry.io/docs/concepts/instrumentation/zero-code/
[arg-http-status]: https://opentelemetry.io/docs/specs/semconv/http/http-spans/#status
[arg-cardinality]: https://opentelemetry.io/docs/specs/otel/metrics/sdk/#cardinality-limits
[arg-billing]: https://docs.datadoghq.com/account_management/billing/custom_metrics/
[arg-metric-model]: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram
[arg-curves]: https://d3js.org/d3-shape/curve
[arg-agent-tools]: https://www.anthropic.com/engineering/writing-tools-for-agents
[arg-dashboards]: https://prometheus.io/docs/practices/consoles/
[arg-components]: https://a2ui.org/concepts/catalogs/
[arg-peak]: https://github.com/CtrlSpice/otel-desktop-viewer/blob/f77ec6d92618dd8ce38cdee392aaa6387bc28b23/desktopexporter/internal/store/metrics/metrics_test.go#L4491-L4566
[arg-exp-merge]: https://github.com/CtrlSpice/otel-desktop-viewer/blob/f77ec6d92618dd8ce38cdee392aaa6387bc28b23/desktopexporter/internal/store/metrics/metrics_test.go#L4584-L4661
[arg-cumulative]: https://github.com/CtrlSpice/otel-desktop-viewer/blob/f77ec6d92618dd8ce38cdee392aaa6387bc28b23/desktopexporter/internal/store/metrics/metrics_test.go#L4663-L4725
[arg-sdk-test]: https://github.com/CtrlSpice/otel-desktop-viewer/blob/f77ec6d92618dd8ce38cdee392aaa6387bc28b23/desktopexporter/internal/store/metrics/sdk_cumulative_test.go#L40-L145
[arg-step]: https://github.com/CtrlSpice/otel-desktop-viewer/blob/f77ec6d92618dd8ce38cdee392aaa6387bc28b23/desktopexporter/internal/frontend/src/components/metrics/Charts/MetricTimeSeriesChart.svelte#L180-L200
[arg-exemplar-ui]: https://github.com/CtrlSpice/otel-desktop-viewer/blob/f77ec6d92618dd8ce38cdee392aaa6387bc28b23/desktopexporter/internal/frontend/src/components/metrics/Detail/SeriesDatapointList.svelte#L382-L408
[arg-exemplar-test]: https://github.com/CtrlSpice/otel-desktop-viewer/blob/f77ec6d92618dd8ce38cdee392aaa6387bc28b23/desktopexporter/internal/frontend/src/components/metrics/Detail/SeriesDatapointList.test.ts#L562-L617
