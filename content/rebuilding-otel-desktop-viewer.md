+++
date = '2026-08-07T12:00:00-07:00'
draft = true
title = "I rebuilt otel-desktop-viewer on top of DuckDB"
description = "and all I got was searchable traces, metrics, and logs. Was really hoping for a t-shirt but I'll take the win."
tags = ['OpenTelemetry', 'otel','otel-desktop-viewer', 'duckdb', 'svelte', 'traces', 'logs', 'metrics']
author = 'Mila Ardath'
+++

`otel-desktop-viewer` is a single binary that shows your OpenTelemetry data locally.
You point your app's OTLP exporter at it, and it opens a browser with your traces, metrics and logs in it.
There's no backend to stand up, no docker compose file, no storage to configure.

Underneath it runs on [DuckDB](https://duckdb.org/), with a Svelte UI on top.

## Why DuckDB

A local debugger has different needs than a tool built for production.
At scale the interface is there to help you find relevant telemetry in a very large pile, and some of that machinery gets in the way locally.
Take a search-first UI that won't run a query until you have narrowed down a service.
That doesn't help when what you have is an attribute or a trace ID and no idea which service produced it.

Storage has the same problem.
Normally you need somewhere to send the data, which means running a backend locally, with Docker and a compose file, before you have seen a single span.
I wanted something like SQLite, except columnar, and that is exactly what DuckDB is.
It compiles into the binary. No sidecar, no daemon, nothing to run alongside it.

This is the whole setup:

```sh
brew install --cask otel-desktop-viewer
otel-desktop-viewer
```

Point your app's OTLP exporter at `localhost:4317` for gRPC or `localhost:4318` for HTTP.
The browser opens on its own, and your spans show up as they arrive.

Having a real analytical database in the binary buys a few things.

You can query your telemetry instead of scrolling through it.
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

Charts come out right, and drawing them doesn't stall the UI.
Quantiles, histogram merges, cumulative and delta handling, and getting a few thousand datapoints down to chart size all happen in the query.

The store stays small.
Every distinct attribute key and value is kept once, and everything else points at it.
On the capture I test against that turns 723,692 attribute rows into 267.

A log record or a metric exemplar takes you straight to the span behind it.
All three signals live in the same store, so following a trace ID across them is a lookup rather than a favour.

Your data is a file, if you want it to be.
By default nothing is written to disk: you look at your data, close the tab, and it's gone.
Pass `--db` and you get a DuckDB file you can upload from CI, send to a colleague, or attach to a bug report.

## Traces

[SCREENSHOT: the whole three-pane view. Trace list on the left with span count and error count badges, waterfall in the middle with one red span among the coloured ones, span details on the right.]

Traces open as a waterfall. The list is on the left, span details on the right, and the trace in these screenshots is two hours long with 5,736 spans.

Errors are designed to stand out.
Span bars get a colour per service, and anything carrying an `Error` status or an exception event drops out of that rotation and takes the error colour instead.
Red always means the same thing, no matter how many services are in play.

[SCREENSHOT: further down the same waterfall, where the work stops. The red spans at the end of a long run of non-red ones, at zero duration. Caption: "Eight red spans out of 5,736."]

Events and links are both clickable.
Event dots sit on the span bar, and clicking one puts the span and the event in the URL, so you can send someone the exact thing you were looking at.
Links take you to the linked span, in whatever trace it lives in.

[SCREENSHOT: the Links panel open with a link expanded, showing the trace ID and span ID as live links, with event dots visible on a span bar behind it.]

## Metrics

[SCREENSHOT: the metrics drawer, showing as many different instrument types as fit in one list, with the type badges and last values visible.]

All five OTel instrument shapes are supported: gauges, counters, up-down counters, histograms and exponential histograms.
The chart you get depends on which one you picked, and you're only offered the aggregations that mean something for it.
I learned a lot of metrics maths so you don't have to.

Histograms get three views of the same data: a heatmap, quantile overlays at p50, p95 and p99, and the bucket distribution itself.

[SCREENSHOT: a histogram on the heatmap view, with the tab control for heatmap / quantiles / distribution visible.]

[SCREENSHOT: the same instrument on the quantiles view, with p99 selected.]

Anything with more than one series overlays by default, with a legend, per-series sparklines, and min/max/average overlays you can switch on.
I have spent way too long toggling those overlays on and off for funsies, because they're pretty and my squirrel brain has needs.

[SCREENSHOT: a multi-series counter on the rate view, legend visible, stat overlays on, series panel showing per-series sparklines with min/max/avg.]

Datapoints that arrived with exemplars say so, and clicking one takes you to the span that produced the number.

[SCREENSHOT: a datapoint row expanded to show its exemplars, with the trace link visible.]

## Logs

[SCREENSHOT: the logs list with a search applied, severity colours across several services, one record selected with its detail pane open.]

Logs are searchable on everything they carry, including severity as a number, so `severityNumber >= 17` gets you ERROR and above without guessing whether the emitter wrote `Error`, `ERROR` or `err`.

A record that arrived with trace context shows its trace and span IDs, and clicking either takes you to that span in its trace.
No copying an ID out of one pane and pasting it into another.

[SCREENSHOT: the detail pane for a record carrying trace context, with the trace ID and span ID visible as links.]

## What's next

**Sharing.**
You can already move the store around, since it's a file.
I still want to build a way to export a slice of what you're looking at, and a way to load someone else's back in.
That waits on the schema settling down, because asking people to trade files whose layout changes every month would be rude.

**Agents.**
The world looks different from when I started this in 2023.
Developer tools now have to serve agents as well as people, and an agent could drive the same query surface the UI uses.
What it can't do is look at the result, so the answer belongs in the viewer where you can check it, rather than in a chat log where you have to take its word for it.
More on that soon.

## Install it

Homebrew, Docker, `.deb` and `.rpm` packages, prebuilt binaries, or `go install`.
The [README](https://github.com/CtrlSpice/otel-desktop-viewer#getting-started) has all of them, and issues and PRs are welcome as always.
