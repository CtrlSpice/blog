+++
date = '2026-11-08'
draft = true
title = '11 UI improvements in otel-desktop-viewer'
type = 'page'
url = '/viewer-ui-improvements/'
+++

The hyperfocus gods have been generous to me lately, but switching activities is hard, so most of that energy has gone directly into otel-desktop-viewer, and ~~very little~~ none has gone into writing about it.
I’m working on longer posts about agent support (that I shipped without telling anyone, sorry 🇨🇦) and upgrading to DuckDB 2 (a native `VARIANT` type y’all).

In the meantime, here’s a tour of UI improvements!

## 1. Look! A new theme!

I loved the previous colour scheme ([Rosé Pine](https://rosepinetheme.com/)), but accessibility is one of my priorities for otel-desktop-viewer, and meeting [WCAG’s text contrast requirements](https://www.w3.org/WAI/WCAG22/Understanding/contrast-minimum.html) was a struggle.

The replacement, [Madoromi](https://madoromi.moonwit.ch/), gets its colours from scans of Pilot Iroshizuku fountain pen inks.
It’s gorgeous, and its consistent colour roles, [palette generation](https://madoromi.moonwit.ch/#palettes/method) and [contrast checks](https://github.com/the-moonwitch/madoromi/blob/main/tests/palettes.test.ts) make it easier to build the same UI for light and dark mode.

You can switch between four seasons, each with Day and Night variants, without changing your service or series colours.

{{< figure src="/images/viewer-ui-improvements/theme-traces-four-seasons.png" alt="The same agent trace in four themes, arranged in a two-by-two grid: Autumn Night and Summer Night above Winter Day and Spring Day. Each service keeps the same colour across all four waterfalls." >}}

## 2. Read structured attributes

I started coming across span attributes as JSON strings (looking at you [gen-ai semantic conventions](https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/README.md)) and reading them as raw strings felt gross.
The details pane now shows their structure with indentation, syntax highlighting and folding.
Below is the same JSON string in v0.5.0 (left) and v0.7.0 (right), with the retries array folded.

{{< figure src="/images/viewer-ui-improvements/json-string-before-after-v05-v07.png" alt="Side-by-side details-pane screenshots labelled Before and After underneath. In v0.5.0, gen_ai.tool.call.result appears as a dense JSON string with escaped quotes and backslashes. In v0.7.0, the same string is displayed with indentation, syntax highlighting and folding, with arguments and result expanded and the retries array folded." >}}

The same view works for native OTLP arrays and maps, represented in OTLP JSON by [`arrayValue`](https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/common/v1/common.proto#L55-L60) and [`kvlistValue`](https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/common/v1/common.proto#L62-L75), respectively.
Below, `agent.tool_calls` on the left is an array of maps, while `agent.context` on the right is a map containing nested maps and an array.

{{< figure src="/images/viewer-ui-improvements/native-array-map-bottom-labels.png" alt="Two details-pane close-ups side by side, labelled Array and Map below the images. On the left, agent.tool_calls is an array with one tool-call map expanded to show a nested error and three retries. On the right, agent.context is a map with the task and token usage expanded and the plan array folded." >}}

### 2′ Search within maps and arrays

Okay so this isn’t entirely a UI feature, but I'm sneaking it in anyway because it's neat.
Let's call it 2′.

You can now query individual values inside maps and arrays.
Let's say you wanted to find agent runs where the second tool call reported at least three retries:

```sql
span.attributes["agent.context"]["tool_calls"]
  [1]["result"]["retries"]::BIGINT >= 3
```

{{< figure src="/images/viewer-ui-improvements/nested-search.png" alt="A nested-attribute query above a colourful, nine-service agent trace. In the details pane, agent.context has its tool_calls array expanded. The first call, search_traces, shows checkout in its service argument and 12 matches in its result. The beginning of the second call shows payment-service in its arguments." >}}

## 3. Copy and download OTLP JSON

Ever wish you could see the raw OTLP payload behind a value?
Now you can!
The JSON tab shows the selected record as [OTLP JSON](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/docs/otlp-json-export.md), with its resource and scope context.
You can fold the JSON, copy it or download it as a file.

{{< figure src="/images/viewer-ui-improvements/otlp-json.png" alt="A wide view of the JSON pane, showing the selected span’s name, IDs and timestamps inside an OTLP document. Resource, scope, attributes and events are folded. The Properties and JSON tabs, Copy and Download controls are visible." >}}

## 4. Import and share telemetry

You can now bring those OTLP files back into the viewer: drop JSON or JSONL files anywhere in the window.
Send your colleague a weird trace and they can drag it right in to see what you see.

{{< video src="/videos/viewer-ui-improvements/trace-export-delete-import-smooth.mp4" poster="/images/viewer-ui-improvements/trace-import-smooth-video-opening.png" width="1440" height="900" alt="Silent recording of exporting and re-importing the Investigate checkout trace. Download trace saves its OTLP JSON, then Delete this trace in the waterfall footer removes it from the list. After a refresh, two traces remain. The downloaded file is dropped into the viewer. Refreshing restores the trace to the list, and selecting it shows its full 14-span waterfall again." >}}

## 5. Compare metric series

The new series table lets you sort and compare series by their attributes, such as CPU core.
Hover over a row to highlight its line on the chart, or click it to inspect the datapoints.
You can select individual datapoints from either the chart or the table.

{{< video src="/videos/viewer-ui-improvements/metric-series-comparison.mp4" poster="/images/viewer-ui-improvements/metric-series-video-opening.png" width="1440" height="900" alt="Silent recording of three CPU series. The table is sorted by CPU core, and hovering over core-3 highlights its lime line. Clicking the spike selects its datapoint in the details pane. Selecting core-1 in the table reveals its datapoints; clicking the first one highlights the corresponding point on the pink line." >}}

{{< hire-cta >}}

## 6. Explore heatmaps and histograms

You can highlight p50, p95 and p99 on the heatmaps while keeping the full distribution visible.
This helps you see whether requests are getting slower across the board or just in the tail.

{{< video src="/videos/viewer-ui-improvements/dense-exponential-heatmap.mp4" poster="/images/viewer-ui-improvements/dense-exponential-heatmap-video-opening.png" width="1440" height="900" alt="Silent recording of a dense exponential-histogram heatmap. Percentiles start hidden. Turning them on reveals p50, p95 and p99; clicking p95 highlights its yellow curve. Selecting a 280 ms bucket at 13:41:30 pins its count of 117 and the column summary, with matching datapoints highlighted in the details pane. Selecting the 14.7 ms bucket in the same column changes the pinned count to 169." >}}

You can also hide the overlays to inspect the distribution on its own.

{{< figure src="/images/viewer-ui-improvements/heatmap-moving-peak.png" alt="A dense exponential-histogram heatmap with a bright latency band that rises, dips and rises again. A sparse violet tail extends below the dip, giving it a funnel shape. The Percentiles checkbox is unchecked and no percentile curves are shown." >}}

The histogram matches the heatmap’s colours for bucket counts.
Selecting a datapoint shows how much it contributes to each bucket in the combined distribution.
Texture marks the selected datapoint’s contribution.

{{< video src="/videos/viewer-ui-improvements/exponential-histogram-contributions.mp4" poster="/images/viewer-ui-improvements/exponential-histogram-video-opening.png" width="1440" height="900" alt="Silent recording of datapoint contributions in an exponential histogram. The details pane lists four checkout datapoints in a 30-second window. Selecting the 13:49:00 datapoint adds textured contributions to the bars. Selecting 13:49:30 changes those contributions. Hovering over the 64–76.1093 ms bucket shows a count of 313, with 25 contributed by the selected datapoint." >}}

## 7. See reporting gaps

Gauge charts now leave gaps when samples stop arriving, while keeping real zero readings on the chart.

{{< figure src="/images/viewer-ui-improvements/gauge-gaps-long-outage.png" alt="Gauge chart with long-outage selected in pink. Its line has a gap from about 05:05 to 05:08 and a connected zero-valued interval from 05:11 to 05:12, marked min 0. A continuous blue series and a cyan series with several gaps remain visible. The details pane shows the selected long-outage row and its datapoints, with 205 in total." >}}

## 8. Find logs and events in the trace timeline

Span events and correlated logs now appear together in the waterfall (both are invited to the [Span Event API deprecation party](https://opentelemetry.io/blog/2026/deprecating-span-events/)).
Hover over the circles to see span activity, with nearby markers sharing a popover.
Choose an event to inspect its attributes, or follow a correlated log to its full record.

{{< video src="/videos/viewer-ui-improvements/trace-activity.mp4" poster="/images/viewer-ui-improvements/trace-activity-video-opening.png" width="1440" height="900" alt="Silent recording of logs and events on the Inspect retries span. Hovering over one circle shows a payment-provider timeout log; the next circle opens a list of nearby logs and span events. Selecting retry.observed reveals retry.attempt of 3 and retry.reason of upstream timeout in the details pane. Selecting the retry-budget log highlights its summary, which opens the full log record when clicked." >}}

## 9. Add new columns to the trace waterfall view

Spans are full of good information!
When you are looking at a trace you might want more info than just the span name and service.
For example, add the agent’s phase and step number as columns to compare them across spans without opening each one.

{{< video src="/videos/viewer-ui-improvements/waterfall-columns.mp4" poster="/images/viewer-ui-improvements/waterfall-columns-video-opening.png" width="1440" height="900" alt="Silent recording of adding columns to a trace waterfall. The column picker opens, its search filters the available fields to agent attributes, and agent.phase and agent.step are selected. Closing the picker reveals both columns alongside each span’s name and service, showing phases such as planning, retrieval and analysis, and step numbers from 1 to 14." >}}

## 10. Customize columns and inspect structured logs

Similar to the trace view, you can now also customize your columns in the log view!
The table also has severity filters and error navigation.
Select a log to inspect its body and expand nested data, such as the tool result and error details below.
Use Collapse all and Expand all to fold or unfold the whole body at once.

{{< video src="/videos/viewer-ui-improvements/log-nested-error-details.mp4" poster="/images/viewer-ui-improvements/log-nested-error-video-opening.png" width="1440" height="900" alt="Silent recording of inspecting a structured log body. Selecting Payment retries found opens the log in the details pane. Expanding tool_call, result, retries, last_error and upstream reveals three retry attempts, a DEADLINE_EXCEEDED error, and the payment-gateway service with a 1500 ms timeout. The body is then collapsed with Collapse all and fully unfolded with Expand all." >}}

## 11. Explore telemetry from the service map

If you’re running your own fleet of services during development, the service map on Home gives you a quick way to find the latest telemetry from a particular service.
Click a service to browse the telemetry it emitted in the details pane.
The results follow your current search and time range.

{{< video src="/videos/viewer-ui-improvements/service-map-browse-telemetry.mp4" poster="/images/viewer-ui-improvements/service-map-video-opening.png" width="1440" height="900" alt="Silent recording of browsing telemetry from Home’s service map. Selecting agent-orchestrator shows three matching traces in the details pane. The Metrics tab shows agent.tool.duration, and the Logs tab lists logs from agent-orchestrator. Opening Investigate checkout completed switches to the log view, with that record’s body, severity and trace context visible in the details pane." >}}

## Other niceties

- Navigation moved to the top bar, leaving more room for your data.
- Scroll-to-top buttons in the logs table, log details and trace waterfall.
- Copy buttons for individual fields and whole records.
- Resizable panes and columns.
- Arrow-key navigation between tabs in the details pane.
- Jump between log errors, including across pages.
- Both ends of long IDs stay visible in narrow waterfall columns.
- Readable storage sizes, with exact byte counts on hover and Copy.
- Choose your own colours for individual services and metric series.
  Change service colours directly from the map on Home.

{{< figure src="/images/viewer-ui-improvements/home-colours.png" alt="Home’s service map with api-gateway selected and its colour picker open. The picker shows a blue colour field, hue slider, hex value, shade swatches and a reset control, with connected services visible behind it." >}}

## Try it out

[Install otel-desktop-viewer](https://github.com/CtrlSpice/otel-desktop-viewer#getting-started), point an OTLP exporter at it and your telemetry shows up in the browser.
Everything shown here is in [0.7.0](https://github.com/CtrlSpice/otel-desktop-viewer/releases/tag/v0.7.0).

The agent-facing changes need a post of their own.
That’s next.
