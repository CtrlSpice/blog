+++
date = '2026-08-07T12:00:00-07:00'
draft = true
title = "I rebuilt otel-desktop-viewer on top of DuckDB"
description = "and all I got was locally searchable traces, metrics, and logs. Was really hoping for that t-shirt everyone keeps talking about, but I'll take the win."
tags = ['OpenTelemetry', 'otel','otel-desktop-viewer', 'duckdb', 'svelte', 'traces', 'logs', 'metrics']
author = 'Mila Ardath'
+++

`otel-desktop-viewer` is a single binary that shows your OpenTelemetry data locally.
Inside, a lightweight Collector receives your telemetry, an embedded [DuckDB](https://duckdb.org/) stores and queries it, and a Svelte UI puts it on screen.
You point your app's OTLP exporter at it, and it opens a browser with your traces, metrics and logs in it.
There's no backend to run, no compose file, and no storage to configure.

## Why DuckDB

Fundamentally, a local debugger has different needs than a tool built for production.
At scale the interface helps you find relevant telemetry in a very large pile, and some of that machinery gets in the way locally.
Take a search-first UI that won't run a query until you have narrowed down a service.
That doesn't help when what you have is an attribute or a trace ID and no idea which service produced it.

Storage has the same problem.
Normally you need somewhere to send the data, and locally that means Docker, a compose file, and a backend to run.
DuckDB is an embeddable columnar database, so I compiled it into the binary and telemetry goes straight into it.

Start it, and it opens in your browser.
Send OTLP to `localhost:4317`.

[SCREENSHOT: the viewer freshly opened with the first few demo spans arriving in the trace list.]

## What the database buys

You can query your telemetry meaningfully instead of just scrolling through it.
The same syntax works on traces, metrics and logs, and you can find a span by an event inside it or by the span it links to.

```
event.name = exception
serviceName = cartservice AND statusCode = Error
duration > 1000000000
name CONTAINS checkout
```

[GIF: typing a query in the search box with autocomplete suggesting fields and operators, then the list filtering down as the query completes.]

A trace with thousands of spans opens fast and stays smooth while you scroll.
Span trees are built by the database: a recursive CTE walks the parent-child links and hands back rows already in the order the waterfall draws them, so nothing in the browser has to assemble a tree.

Quantiles, histogram merges, and cumulative and delta handling all happen in the query, and so does getting a few thousand datapoints down to chart size.
Drawing a chart doesn't stall the UI.

Every distinct attribute key and value is stored once, and everything else points at it.
On the capture I test against that turns 723,692 attribute rows into 267.

A log record or a metric exemplar takes you straight to the span behind it.
All three signals live in the same store, so following a trace ID across them is a lookup.

Your data can be a file.
By default nothing is written to disk: you look at your data, close the tab, and it's gone.
Pass `--db` and you get a DuckDB file you can upload from CI, send to a colleague, or attach to a bug report.

## Traces

[SCREENSHOT: the whole three-pane view of a demo trace. Trace list on the left with its span count and error count badges, waterfall in the middle with the failing service's span in red among the coloured ones, span details on the right.]

Traces open as a waterfall. The list is on the left, span details on the right, and the trace in these screenshots has NNN spans in it.

Errors are designed to stand out.
Span bars get a colour per service, and anything carrying an `Error` status or an exception event drops out of that rotation and takes the error colour instead.
Red always means the same thing, no matter how many services are in play.

[SCREENSHOT: the same waterfall scrolled to the failure, with the red spans among the non-red ones. Turn on a flagd failure flag first so there is something to show. Caption: "NN red spans out of NNN."]

Events and links are both clickable.
Event dots sit on the span bar, and clicking one puts the span and the event in the URL, so you can send someone the exact thing you were looking at.
Links take you to the linked span, in whatever trace it lives in.

[SCREENSHOT: the Links panel open with a link expanded, showing the trace ID and span ID as live links, with event dots visible on a span bar behind it. The demo's queue-based services are the likeliest place to find a span carrying links.]

## Metrics

[SCREENSHOT: the metrics drawer with as many different instrument types as the demo emits in one list, badges and last values visible.]

All five OTel instrument shapes are supported: gauges, counters, up-down counters, histograms and exponential histograms.
The chart you get depends on which one you picked, and you're only offered the aggregations that mean something for it.
I learned a lot of metrics maths so you don't have to.

Histograms get three views of the same data: a heatmap, quantile overlays at p50, p95 and p99, and the bucket distribution itself.

[SCREENSHOT: one of the demo's latency histograms on the heatmap view, with the heatmap / quantiles / distribution tabs visible.]

[SCREENSHOT: the same histogram on the quantiles view with p99 selected, so it reads as the same metric asking a different question.]

Anything with more than one series overlays by default, with a legend, per-series sparklines, and min/max/average overlays you can switch on.
I have spent way too long toggling those overlays on and off for funsies, because they're pretty and my squirrel brain has needs.

[SCREENSHOT: a demo counter with several series on the rate view, legend visible, stat overlays on, series panel showing per-series sparklines with min/max/avg.]

Datapoints that arrived with exemplars say so, and clicking one takes you to the span that produced the number.

[SCREENSHOT: a datapoint row expanded to show its exemplars with the trace link visible. Check the demo actually emits exemplars before planning on this one.]

## Logs

[SCREENSHOT: the logs list with a search applied, severity colours across several demo services, one record selected with its detail pane open.]

Logs are searchable on everything they carry, including severity as a number, so `severityNumber >= 17` gets you ERROR and above without guessing whether the emitter wrote `Error`, `ERROR` or `err`.

A record that arrived with trace context shows its trace and span IDs, and clicking either takes you to that span in its trace.
No copying an ID out of one pane and pasting it into another.

[SCREENSHOT: the detail pane for a demo log record carrying trace context, with the trace ID and span ID visible as links.]

## What's next

**Sharing.**
You can already move the store around, since it's a file.
I still want to build a way to export a slice of what you're looking at, and a way to load someone else's back in.
That waits on the schema settling down, because asking people to trade files whose layout changes every month would be rude.

**Agents.**
The world of observability looks different from when I started this project in 2023.
Developer tools now have to serve agents as well as people, and an agent could drive the same query surface the UI uses.
It can't look at the result, so the answer goes in the viewer, where you can check it.
More on that soon.

Not a Homebrew person? The [README](https://github.com/CtrlSpice/otel-desktop-viewer#getting-started) has Docker, `.deb` and `.rpm` packages, prebuilt binaries, and `go install`.
Issues and PRs are welcome, as always.
