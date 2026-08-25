+++
date = '2026-08-24T12:00:00-07:00'
title = "I rebuilt otel-desktop-viewer on top of DuckDB"
description = "and all I got was locally searchable traces, metrics, and logs."
summary = "and all I got was locally searchable traces, metrics, and logs."
tags = ['OpenTelemetry', 'otel','otel-desktop-viewer', 'duckdb', 'svelte', 'traces', 'logs', 'metrics']
author = 'Mila Ardath'
[cover]
  image = "hero.png"
  # The image is a page-bundle resource; without this the og:image tag
  # resolves it against the site root and 404s.
  relative = true
  alt = "A trace waterfall in otel-desktop-viewer: thirty spans across six services, each service in its own colour, with the span detail pane open on the right."
  hiddenInSingle = true
  hiddenInList = false
+++

Was really hoping for a lousy t-shirt, but I'll take the win.
The project crossed 1,000 stars on GitHub yesterday, and [v0.5.0](https://github.com/CtrlSpice/otel-desktop-viewer/releases/tag/v0.5.0) just went out the door, full of bug fixes.
Together, that felt like a good excuse for a proper reintroduction, three years and one DuckDB rewrite after I shipped the first version.

## What's an otel-desktop-viewer anyway?

![A trace waterfall in otel-desktop-viewer: thirty spans across six services, each service in its own colour, with the span detail pane open on the right.](hero.png)

`otel-desktop-viewer` is a single binary that lets you search and query your OpenTelemetry traces, metrics, and logs locally in your browser.
Inside, a lightweight Collector receives your telemetry, an embedded [DuckDB](https://duckdb.org/) stores and queries the data, and a Svelte UI puts it on screen.
There's no backend to run, no compose file, and no storage to configure.

Fundamentally, a local debugger has different needs than a tool built for production.
At scale the interface helps you find relevant telemetry in a big ol' pile of data.
Some of that machinery gets in the way when working locally.
For example, take a search-first UI that won't run a query until you have narrowed down a service.
This doesn't help when we are:

- trying to get familiar with a new system
- tracking down an event with no idea who produced it
- holding a trace ID that the UI won't look up without a service name

## Why DuckDB

Storage has the same problem.
OTLP data needs somewhere to go, and every backend you could send it to wants its own deployment.
DuckDB is an embeddable columnar database, so I compiled it into the binary and telemetry goes straight into it.

Here's what that bought, and how:

- **You can query your telemetry meaningfully instead of just scrolling through it.**
  The same syntax works on traces, metrics and logs, and you can find a span by an event inside it or by the span it links to.

- **A trace with thousands of spans opens fast and stays smooth while you scroll.**
  Span trees are built by the database: a recursive CTE walks the parent-child links and hands back rows already in the order the waterfall draws them, so nothing in the browser has to assemble a tree.

- **Drawing a chart doesn't stall the UI.**
  Quantiles, histogram merges, and cumulative and delta handling all happen in the query, and so does getting a few thousand datapoints down to chart size.
  Y'all, metrics maths with DuckDB is so cool, and this is me teasing the next blog post with the subtlety of a possum.

- **The store stays small.**
  Every distinct attribute key and value is stored once, and everything else points at it.
  On the capture I test against that turns 723,692 attribute rows into 267.

- **A log record or a metric exemplar takes you straight to the span behind it.**
  All three signals live in the same store, so following a trace ID across them is a lookup.

- **Your data can be a file.**
  By default nothing is written to disk: you look at your data, close the tab, and it's gone.
  Pass `--db` and you get a DuckDB file you can come back to.

Some examples of things you can type into the search box:

```
event.name = exception
service.name = cart AND statusCode = Error
duration > 1000000000
name CONTAINS checkout
```

## Traces

![The three-pane traces view: the trace list on the left with span count badges, a waterfall of coloured span bars in the middle, and the span detail pane on the right.](traces-three-pane.png)

Traces open as a waterfall. The list is on the left, span details on the right, and this checkout trace runs 74 spans across 12 services.

Errors are designed to stand out.
Span bars get a colour per service, and anything carrying an `Error` status or an exception event drops out of that rotation and takes the error colour instead.
Red always means the same thing, no matter how many services are in play.

{{< figure src="traces-errors.png" alt="The same waterfall with the sidebar collapsed: red error spans stand out among the coloured ones, and the exception event is open in the detail pane showing its message, stacktrace, and type." caption="8 red spans out of 93." >}}

Events and links are both clickable.
Event dots sit on the span bar, and clicking one puts the span and the event in the URL, so you can send someone the exact thing you were looking at.
Links take you to the linked span, in whatever trace it lives in.

![The Links panel open for a consumer span, showing the linked trace ID and span ID as clickable links.](traces-links.png)

## Metrics

![The metrics drawer filtered to five metrics, showing gauge, counter, up-down counter, and histogram badges with last values, and a rate chart open for the selected counter.](metrics-instruments.png)

All five OTel instrument shapes are supported: gauges, counters, up-down counters, histograms and exponential histograms, though the demo app in these screenshots only emits the first four.
The chart you get depends on which one you picked, and you're only offered the aggregations that mean something for it.
I learned a lot of metrics maths so you don't have to.

Histograms get three views of the same data: a heatmap, a quantile view you can flip between p50, p95 and p99, and the bucket distribution itself.

![A latency histogram on the heatmap view, with the heatmap, quantiles, and histogram tabs at the top and the per-series list on the right.](metrics-heatmap.png)

![The same histogram on the quantiles view with p99 selected, showing stepped per-series quantile lines.](metrics-quantiles.png)

Anything with more than one series overlays by default, with a legend, per-series sparklines, and min/max/average overlays you can switch on.
I have spent way too long toggling those overlays on and off for funsies, because they're pretty and my squirrel brain has needs.

![A six-series counter on the rate view with a pinned crosshair: a per-series value table, min and max markers, a slope readout, and per-series sparklines in the panel on the right.](metrics-series.png)

Datapoints that arrived with exemplars say so, and clicking one takes you to the span that produced the number.

![A histogram datapoint expanded to show five exemplars, each with its value, timestamp, and clickable trace and span links.](metrics-exemplars.png)

## Logs

![The logs list filtered by severityNumber >= 17, with error badges on each record and the selected record's detail pane showing an exception message and stacktrace.](logs-severity.png)

Logs are searchable on everything they carry, including severity as a number, so `severityNumber >= 17` gets you ERROR and above without guessing whether the emitter wrote `Error`, `ERROR` or `err`.

A record that arrived with trace context shows its trace and span IDs, and clicking either takes you to that span in its trace.
No copying an ID out of one pane and pasting it into another.

![The detail pane for a log record that arrived with trace context: the trace ID and span ID render as underlined links above the record's attributes.](logs-trace-context.png)

## Coming up

**Sharing:**
You can already move the store around, since it's a file.
I still want to build a way to export a slice of what you're looking at, and a way to load someone else's back in.
That waits on the schema settling down, because asking people to trade files whose layout changes every month would be rude.

**Agents:**
The world of observability looks different from when I started this project in 2023.
Developer tools now have to serve agents as well as people, and an agent could drive the same query surface the UI uses.
Instead of handing you a summary you have to take on faith, it can show you exactly where to look in your own data, so you can see the problem for yourself.

More on that soon.

## Try it out

Install instructions are in the [README](https://github.com/CtrlSpice/otel-desktop-viewer#getting-started).
Issues and PRs are welcome, as always.
