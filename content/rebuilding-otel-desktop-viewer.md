+++
date = '2026-08-07T12:00:00-07:00'
draft = true
title = "I rebuilt otel-desktop-viewer on top of DuckDB"
description = "and all I got was searchable traces, metrics, and logs. Was really hoping for a t-shirt but I'll take it."
tags = ['OpenTelemetry', 'otel','otel-desktop-viewer', 'duckdb', 'svelte', 'traces', 'logs', 'metrics']
author = 'Mila Ardath'
+++

Hey folks,

This post has been a long[^1] time in the making.
When I first released `otel-desktop-viewer` three years ago, I was new to OpenTelemetry and wanted a way into a large, sometimes confusing ecosystem, minus the whole side quest of standing up a full backend.

The thing is, a local debugger has fundamentally different needs than an observability tool built for production.
At scale, the interface exists to help you find relevant telemetry in, for euphemism's sake, a big ol' pile.
Some of that machinery gets in the way locally.
Take a search-first UI that won't run a query until you have narrowed down a service.
That doesn't help when what you have is an attribute or a trace ID and no idea which service produced it.
So my tiny viewer did exactly one thing: it showed you your trace waterfalls in a browser, and left it at that.

Apparently there was enough need for a local viewer that did that specific thing.
People started using my toy project for real work, filing issues, contributing, and generally being kind and helpful about it.
It took me a while to notice I'd become a maintainer (plus a bit longer to stop feeling like a dog in a labcoat about it).
I did start thinking about how to turn my toy into a tool.

Luckily, the whole "thinking" bit wasn't too arduous, what with my GitHub issues doing the heavy lifting:

{{< issue meta="#6.28318 · opened three years ago" >}}Cool! Do metrics and logs next!{{< /issue >}}
{{< issue meta="#2i · reopened, twice · Totally fair. I did." >}}Respectfully, you broke CI *again*{{< /issue >}}
{{< issue meta="#6.62607015e-34 · opened Tuesday" >}}[feature] please let me persist my data?{{< /issue >}}
{{< issue meta="#1.61803 · opened Tuesday" >}}[feature] please let me search?{{< /issue >}}
{{< issue meta="#6.02214076e23 · also Tuesday" >}}[feature] please let me filter?{{< /issue >}}

Last fall, a batch at the [Recurse Center](https://www.recurse.com/) gave me the time and focus to start The Big Rewrite(tm), which took a couple of releases and several months but is finally done.

Today, `otel-desktop-viewer` is still a single binary that shows you your telemetry locally, and asks as little of you as possible.
Underneath, there's now full support for OTLP traces, metrics, and logs, running on a DuckDB analytical database that can live in memory or on disk, with a new Svelte UI that surfaces search, sort, and filter.

Four of those five issues are closed. The CI one is evergreen and stays open on principle.

## Traces

The layout will look familiar to returning users.
That said, the trace in these screenshots is two hours long with 5,736 spans, and it still opens fast and doesn't chug when you interact with it.

[SCREENSHOT: the whole three-pane view. Left drawer showing the trace card with its span count and error count badges, waterfall in the middle with one red span among the coloured ones, right pane on the Events tab. The point is that the counts are visible before you open anything. Caption: "One trace, one drawer card, and a span count and error count before you open anything."]

All three signals share one search grammar.
Twelve operators, including regex, `CONTAINS`, `IN`, and starts-with and ends-with.
`AND`, `OR`, and parentheses when you want to group things.
It knows which operators make sense for which field and says so when they don't match, and a bare word with no field at all just searches everything.
The syntax carries everywhere, even though the fields change per signal.
On spans the fields reach past the span itself: `event.name`, `event.timestamp`, `link.traceID`, `link.spanID`, so you can find a span by what happened inside it or by what it points at.
`duration` is computed when you ask rather than stored, so you can compare against it.

You can sort the list by start time, duration, root span name, service, span count, or error count.
The time window is set once for the whole app, not per page, so narrowing it here narrows it everywhere else too.

[SCREENSHOT: the drawer doing its job. A query typed into the trace search box with the list filtered down, and ideally the sort control or the time range picker open so both are visible. This is the only paragraph without a picture, and the claim it needs to support is "one search box, one time window, everywhere".]

Errors are designed to stand out.
Span bars get a colour per service, or per span name when there's only one service, so you can tell where a span came from without reading its row.
Anything carrying an `Error` status or an exception event drops out of that rotation and takes the error colour instead.
Red always means the same thing, no matter how many services are in play.

[SCREENSHOT: further down the same waterfall, where the work stops. The red spans at the end of a long run of non-red ones, at zero duration, with the Fields pane showing status code Error. Caption: "Eight red spans out of 5,736. The work stops where the red starts, and both of these ran for 0 ns."]

Events and links are both clickable.
Event dots sit right on the span bar, and selecting one puts both the span and the event in the URL, so you can hand someone the exact thing you were looking at.
Links show their attributes and trace state, with the trace ID and span ID live: click either and you land on the linked span, in whatever trace it lives in.

[SCREENSHOT: the Links panel open with a link expanded, showing its attributes and the trace ID and span ID as live links, ideally with event dots visible on a span bar behind it so one shot covers both.]

Click a trace ID on a log record, or an exemplar on a metric datapoint, and you arrive at the span rather than the top of the trace.

I'm proud of the span tree, but it's a database story, so I'll circle back to it.

## Metrics

So, we have metrics now.

[SCREENSHOT 1: the metrics drawer, showing as many different instrument types as you can fit in one list. Needs the badges (type, temporality, monotonic, series count) and the last value with units. Proves the range of instrument types the viewer handles.]

Each row shows the metric's name and service, its last value with units, and a set of badges: instrument type, cumulative or delta, monotonic or not, and the number of series underneath.

Which aggregations you're offered depends on the instrument, and you only get the ones that mean something.
A cumulative monotonic sum can draw as a rate; a gauge can't, because it isn't cumulative.
With more than one series you can average them, and total them if they're sums, but not if they're gauges, since a total of unrelated gauge readings doesn't mean anything.
I learned a lot of metrics maths so you don't have to.

Both kinds of histogram, explicit-bucket and exponential, get the same three views: a heatmap, quantile overlays at p50, p95 and p99, and the bucket distribution itself.

[SCREENSHOT 2: a histogram on the heatmap view, with the tab control for heatmap / quantiles / distribution visible so it's obvious there are three.]

[SCREENSHOT 3: the same instrument as shot 2, on the quantiles view, with p99 selected. The point is same data, different question, so it should be recognisably the same metric.]

There's also a scope switch: whole window, or snapshot.
Whole window folds every datapoint in the visible range into one distribution.
Snapshot shows you what it looked like at a single instant.

Anything with more than one series overlays by default, with a legend to toggle them individually, and every series gets its own sparkline in the panel.
You can add an aggregate line across all of them, or just the ones you've checked.
You can turn on min, max and average overlays.
The per-series numbers are computed over the time window, not over whatever points got drawn, so they don't move when you resize the chart.
I have spent way too long toggling those overlays on and off for funsies, because they're pretty and my squirrel brain has needs.

[SCREENSHOT 4: a multi-series instrument with several series overlaid, the legend visible, stat overlays turned on, and the series panel showing per-series sparklines with min/max/avg. If a counter on the rate view can carry this shot, better, since it covers the raw/sum/avg/rate control at the same time.]

Datapoints can carry exemplars, and when they do the chart says so.
Click one and you land on the span that produced the number.

[SCREENSHOT 5: a datapoint row expanded to show its exemplars, with the trace link visible. Optionally a second shot of where you land, though the Traces section already shows a waterfall so it may be redundant.]

## Logs

We also have queryable logs!
The fields are `timestamp`, `observedTimestamp`, `traceID`, `spanID`, `severityText`, `severityNumber`, `body`, `eventName` and `flags`, plus any attribute the record arrived with.
Having both timestamps means you can ask about the gap between when something happened and when it turned up.
Severity is a number as well as a name, so `severityNumber >= 17` gets you everything at ERROR and above without matching text that might say `Error`, `ERROR` or `err` depending on who emitted it.

[SCREENSHOT: the logs list with a search applied, severity colours visible across several services, and one record selected with its detail pane open. Should show that the list is dense and readable at a glance.]

Trace context survives the trip.
A record that arrived with a trace ID and span ID shows both, and clicking either one lands you on that span, in its trace, with the tree already opened around it.
No copying an ID out of one pane and pasting it into another.

[SCREENSHOT: the detail pane for a record that carries trace context, with the trace ID and span ID both visible as links, alongside severity, body and resource attributes.]

## DuckDB

Everything above runs on [DuckDB](https://duckdb.org/), compiled into the binary the way SQLite would.
It's built for analytics, not transactions, and a viewer mostly scans and aggregates.

### In memory, or on disk

The default is in-memory.
You run the binary, point your app at it, look at your data, close the tab, and it's gone.
Pass `--db` and you get a file instead.
The data outlives the process, so closing the tab doesn't throw anything away.

Either way there's a cap.
`--db-max-size` defaults to 512MB in memory and 2GB on disk, and `0` turns it off.
Over the cap, the oldest ten percent of each signal gets pruned, and the UI can warn you before you get there.

### What the database bought me

**Attributes are a dictionary.**
Every distinct key, value, type and scope in the database gets one row, addressed by a hash of its own contents, and anything with attributes stores an array of ids into it.
On the capture I test against, 723,692 attribute rows collapse to 267.
DuckDB's part is that `uuid[]` is a native type, so the reference costs nothing to store or join through.

**Span trees come out of a query.**
Building the parent-child tree used to be Go code doing bookkeeping.
Now a recursive CTE walks the links and carries a sort-path array down the tree, so rows arrive already in the order the UI draws them.
The frontend never learns what a span tree is.

**The metric arithmetic is SQL.**
Quantiles, histogram merges, delta and cumulative handling, and the reduction that fits a few thousand datapoints into a chart all happen in the query.
The browser gets numbers to draw rather than a dataset to process.

## What's next

**Handing the store to someone else.**
The DuckDB store is already a file.
That means the interesting half of "share your telemetry" is done by happenstance, so you can attach it to a bug report, hand it to a teammate, or open it again next Tuesday.
It still needs emit and ingest as real operations, and I haven't picked a wire format yet.

**Agents.**
The RPC surface the UI talks to would work just as well for an agent.
It could run the queries.
It could not look at what came back.
So the answer belongs in the viewer, as something you can check, instead of in a chat log as a sentence you have to believe.

Both of these wait on my schema.
It's still greenfield and still churning, so sharing comes after migrations do.

## Install it

Homebrew:

```sh
brew tap ctrlspice/otel-desktop-viewer
brew install --cask otel-desktop-viewer
```

Or Docker, if you'd rather not install anything:

```sh
docker run -p 8000:8000 -p 4317:4317 -p 4318:4318 ghcr.io/ctrlspice/otel-desktop-viewer:latest
```

`.deb` and `.rpm` packages go out with every stable release through [GemFury](https://gemfury.com/), and Windows builds along with macOS and Linux tarballs are attached to the [GitHub release](https://github.com/CtrlSpice/otel-desktop-viewer/releases/latest).
The exact `apt` and `dnf` setup is in the [README](https://github.com/CtrlSpice/otel-desktop-viewer#getting-started).

`go install` works too, but DuckDB is a C++ library compiled into the binary, which means extra steps on Windows.
The [README](https://github.com/CtrlSpice/otel-desktop-viewer#getting-started) covers it.

However you get it, the UI is at `localhost:8000` by default, and the OTLP receivers are on `localhost:4317` for gRPC and `localhost:4318` for HTTP.
Run the binary and it opens the browser for you.
All three are flags (`--browser-port`, `--grpc`, `--http`) if you need them somewhere else, and `--host` if you need it off localhost.

Issues and PRs are welcome, as always.

[^1]: According to `otel-desktop-viewer` users, this is only true for very long[^1] values of long[^1].
