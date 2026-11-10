+++
date = '2026-11-08'
draft = true
title = '10ish UI improvements in otel-desktop-viewer'
type = 'page'
url = '/viewer-ui-improvements/'
+++

The hyperfocus gods have been generous to me lately, but switching activities is hard, so most of that energy has gone directly into otel-desktop-viewer, and ~~very little~~ none has gone into writing about it.
I’m working on longer posts about agent support (that I shipped without telling anyone, sorry) and upgrading to DuckDB 2 (a native `VARIANT` type y’all).

In the meantime, here’s a tour of UI improvements, in no particular order.

## Search inside attributes

You can now search inside nested maps and arrays.
In this synthetic agent example, the query finds runs where the second tool call reported at least three retries:

```sql
span.attributes["agent.context"]["tool_calls"]
  [1]["result"]["retries"]::BIGINT >= 3
```

{{< figure src="/images/viewer-ui-improvements/nested-search.png" alt="A nested-attribute query above a colourful, nine-service agent trace. The Details pane shows both tool-call objects, including the second call’s result with three retries and a 1,500-millisecond timeout." >}}

In Details, you can fold away the parts of a nested attribute you don’t need, with indentation and syntax highlighting to make the rest easier to follow.
JSON strings can be formatted too, but [Copy still gives you the original text](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/desktopexporter/internal/frontend/src/themes/README.md#typography-and-data-fidelity).

The JSON tab shows the selected record as [OTLP JSON](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/docs/otlp-json-export.md), with its resource and scope context.
You can fold the JSON, copy it or download it as a file.

{{< figure src="/images/viewer-ui-improvements/otlp-json.png" alt="A wide view of the JSON pane, showing the selected span’s name, IDs and timestamps inside an OTLP document. Resource, scope, attributes and events are folded. The Properties and JSON tabs, Copy and Download controls are visible." >}}

## Compare metric series

The new series table lets you sort and compare series by their attributes.
Hover over a row to highlight its line on the chart, or click it to inspect the datapoints.

{{< figure src="/images/viewer-ui-improvements/metric-series-comparison-mono.png" alt="Three CPU series on a line chart. A soft lavender monospaced label, Same timestamp, points to the comparison tooltip inside a dashed outline. The series table is sorted by CPU core, with core-3 selected and its datapoints visible below." caption="Compare three CPU cores at the same point in time, with the selected core’s datapoints alongside." >}}

## See reporting gaps

Gauge charts now leave gaps when samples stop arriving, while keeping real zero readings on the chart.

{{< figure src="/images/viewer-ui-improvements/gauge-gaps.png" alt="Three Gauge series with independent reporting outages. Two lines break where samples are missing, while a continuous comparison line and a recorded interval of zeros remain connected." >}}

## Logs and events in the trace timeline

Span events and correlated logs now appear together in the waterfall (both are invited to the [Span Event API deprecation party](https://opentelemetry.io/blog/2026/deprecating-span-events/)).
Nearby markers share a popover where you can choose an event or log, with links to the full log records.

{{< figure src="/images/viewer-ui-improvements/span-activity-panels-dot.png" alt="The agent waterfall with an activity popover open on the Inspect retries span. A lavender Logs + events label and arrow point to the activity dot. Dashed outlines highlight the popover listing nearby span events and correlated logs, and the Events panel in Details." >}}

## Choose your waterfall columns

You can now choose which fields and attributes appear as columns in the trace waterfall.
Here I’ve added the agent’s phase and step number, so I can compare them across spans without opening each one.

{{< figure src="/images/viewer-ui-improvements/waterfall-columns-mono.png" alt="A multi-service agent waterfall with agent.phase and agent.step columns enclosed by a soft lavender dashed outline. A monospaced Custom columns label and curved arrow point to them. Rows show planning, retrieval, analysis and answer steps." >}}

## A better view of your logs

Here I’ve added columns for the agent’s phase and tool name.
The full tool result is in the log body, with nested error details you can expand and fold.
The table also has severity filters and error navigation.

{{< figure src="/images/viewer-ui-improvements/logs-structured-body.png" alt="Synthetic agent logs with custom agent.phase and tool.name columns. The selected inspect_span log has its structured body open alongside the table, showing tool_call.result.retries.last_error and nested upstream service and timeout details. Arguments, evidence and span details are folded." >}}

## Start with a service

Home’s service map shows how your services connect.
Click a service and you can browse the telemetry it emitted right there in Details.
The results follow your current search and time range, and you can open individual records to inspect them.

{{< figure src="/images/viewer-ui-improvements/service-overview-mono.png" alt="Home with api-gateway selected in the service map and its Metrics tab open in Details. Lavender labels and curved arrows mark the selected service and its matching telemetry, each enclosed by a dashed outline. Traces, Metrics and Logs tabs show counts for that service." >}}

## Import and share telemetry

You can now import telemetry from OTLP JSON or JSONL files: drop them anywhere in the viewer or choose them from Home.
[Downloads use OTLP JSON too](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/docs/otlp-json-export.md), keeping the resource and scope context with the data so you can send someone what you’re looking at and they can open it in their own viewer.

{{< figure src="/images/viewer-ui-improvements/import.png" alt="Home with the Getting started pane open. Below the pink axolotl mascot is a box for choosing telemetry JSON files or dropping them into the viewer; the service map remains visible alongside it." >}}

## Heatmaps and histograms

You can highlight p50, p95 and p99 on the heatmaps while keeping the full distribution visible, or hide the overlays to inspect it on its own.
Selecting a datapoint shows how much it contributes to each bucket in the combined distribution.

{{< figure src="/images/viewer-ui-improvements/heatmap-dense.png" alt="A dense exponential-histogram heatmap with three shifting latency distributions. Violet, pink and pale-rose cells join without gaps, with green p50, yellow p95 and red p99 curves highlighted." caption="See whether requests are getting slower across the board or just in the tail, with p50, p95 and p99 highlighted." >}}

{{< figure src="/images/viewer-ui-improvements/heatmap-moving-peak.png" alt="A dense exponential-histogram heatmap with a bright latency band that rises, dips and rises again. A sparse violet tail extends below the dip, giving it a funnel shape. The Percentiles checkbox is unchecked and no percentile curves are shown." caption="Percentile overlays can be switched off to show the distribution on its own." >}}

{{< figure src="/images/viewer-ui-improvements/histogram.png" alt="A latency histogram with violet and pink distribution bars, percentile markers and a textured contribution from the selected datapoint. Its series and datapoints are visible in Details." caption="The histogram view uses the same count colours, with texture marking the selected datapoint’s contribution." >}}

## Theme

After too much time fighting Rosé Pine’s contrast, I switched to [Madoromi](https://madoromi.moonwit.ch/), based on Pilot Iroshizuku fountain pen inks.
You can switch between four seasons, each with Day and Night variants, without changing your service or series colours.
The fonts are still [Atkinson Hyperlegible Next and Mono](https://brailleinstitute.org/freefont/).

{{< figure src="/images/viewer-ui-improvements/theme-traces-four-seasons.png" alt="The same agent trace in four themes, arranged in a two-by-two grid: Autumn Night and Summer Night above Winter Day and Spring Day. Each service keeps the same colour across all four waterfalls." >}}

## Other niceties

- Navigation moved out of the left drawer and into the top bar.
- Scroll-to-top buttons in the logs table, log details and trace waterfall.
- Copy buttons for individual fields and whole records.
- Resizable panes and columns.
- Arrow-key navigation between Details tabs.
- Jump between log errors, including across pages.
- Both ends of long IDs stay visible in narrow waterfall columns.
- Readable storage sizes, with exact byte counts on hover and Copy.
- Choose your own colours for individual services and metric series.

{{< figure src="/images/viewer-ui-improvements/home-colours.png" alt="Home’s service map with api-gateway selected and its colour picker open. The picker shows a blue colour field, hue slider, hex value, shade swatches and a reset control, with connected services visible behind it." caption="Choose service colours directly from the map on Home." >}}

## Try it out

[Install otel-desktop-viewer](https://github.com/CtrlSpice/otel-desktop-viewer#getting-started), point an OTLP exporter at it and your telemetry shows up in the browser.
Everything shown here is in [0.7.0](https://github.com/CtrlSpice/otel-desktop-viewer/releases/tag/v0.7.0).

The agent-facing changes need a post of their own.
That’s next.
