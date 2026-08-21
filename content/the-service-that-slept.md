+++
date = '2026-08-21T09:20:00-07:00'
draft = true
title = "The Service That Slept"
subtitle = "One container lost three hours overnight, and I found them in my laptop's power log"
tags = ['OpenTelemetry', 'Otel', 'clock skew', 'PHP', 'otel-desktop-viewer']
author = 'Mila Ardath'
+++

## The thing I noticed

I left the OpenTelemetry Demo pointed at [otel-desktop-viewer](https://github.com/CtrlSpice/otel-desktop-viewer) overnight, mostly to have a realistic pile of data to take screenshots of in the morning.
When I opened it up I had 2.3 million spans across 434,198 traces, which was more than enough.
What I did not have was a set of traces I could photograph.

A lot of the checkout traces looked wrong in a very specific way.
The `get_quote` spans ran at the very beginning, then nothing happened for a long time, and then every other span in the trace — including the root — fired in a clump at the end.
Some traces were completely normal.
Most were not.

Here is the same journey through the shop, four times, at four points in the night.

![otel-desktop-viewer showing a checkout trace at 19:46, before the first sleep, spanning 70 milliseconds](/images/the-service-that-slept/fig2a-normal.png)

![The same trace shape at 21:20, with the waterfall's time axis now running to 50 minutes](/images/the-service-that-slept/fig2b-50min.png)

![The same trace shape at 04:11, with the time axis now spanning two hours and ten minutes](/images/the-service-that-slept/fig2c-130min.png)

![The same trace shape at 08:49, with the time axis spanning three hours and twenty-two minutes and every span bar compressed against the right edge](/images/the-service-that-slept/fig2d-202min.png)

Each of those is 159 spans doing about 70 milliseconds of work.
The only thing changing is the axis.

My first instinct was that I had broken something in my own query layer, because I had spent the previous day rewriting how traces are fetched.
That instinct was wrong, and the way it was wrong turned out to be more interesting than the bug I went looking for.

## Measuring it before explaining it

The population splits cleanly.
For every trace containing both a `quote` span and a span from any other service — 2,262 of them — I took the difference between when each group started.

| skew between `quote` and everyone else | traces |
| --- | ---: |
| under a second | 15 |
| one to sixty minutes | 129 |
| over an hour | 968 |

So it is not an occasional glitch.
It is nearly everything, and the 15 clean traces are all from early in the night.

![A trace in otel-desktop-viewer with the three quote spans at the zero mark and every other service's span bar pinned to the right edge, three hours and twenty-two minutes later](/images/the-service-that-slept/fig1-stranded-quote.png)

The shape of an affected trace is stark once you split it by service.
In the trace above, the three `quote` spans sit alone at the origin, and all 156 remaining spans — 44 frontend, 31 cart, 24 product-catalog, 18 proxy, 17 checkout, and the rest scattered across currency, email, shipping, payment, flagd and the load generator — sit together 12,115 seconds later.
Not scattered across the gap.
Together, in one clump, on the far side of three hours and twenty-two minutes of nothing.

That was the detail that redirected me.
A bug in my tree reconstruction would scramble spans, or drop them, or mis-parent them.
It would not sort ten services into exactly two piles and put one language in the first pile.

## The offsets are suspiciously round

Sorting the affected traces by how far the root had drifted, the largest offenders all showed the same number: 12,115.2 seconds.
Not similar numbers.
The same number, to a tenth of a second, across dozens of unrelated traces.

Random clock drift does not do that.
Drift accumulates continuously, so two traces recorded twenty minutes apart should differ by a little.
A constant offset across a batch means the clock did not drift at all — it jumped, once, and then kept perfect time afterward.

Which raises the obvious question of what would make a clock jump by three hours and twenty minutes on a machine sitting on my desk.

## The power log

My laptop had been going to sleep all night.
Not one long sleep, but dozens of short ones: idle sleeps, maintenance sleeps, the usual macOS behaviour when nobody is typing.

I pulled every sleep event from `pmset -g log` inside the capture window and added up the durations.
Eighteen suspends, 12,153 seconds total.

The largest observed skew was 12,115 seconds.

That is a difference of 38 seconds, or about a third of one percent, between a number derived entirely from span timestamps and a number derived entirely from the operating system's power log.
The two measurements have nothing to do with each other.
Neither was fitted to the other.

![A chart with two staircase lines lying almost exactly on top of each other: observed clock skew measured from span timestamps, and cumulative host sleep read from the power log, with each individual sleep event marked along the bottom](/images/the-service-that-slept/fig3-power-correlation.png)

Checking it point by point rather than just at the endpoint:

| elapsed | observed skew | cumulative sleep | difference |
| ---: | ---: | ---: | ---: |
| 1.53 h | 2,969 s | 2,979 s | −10 s |
| 2.67 h | 4,508 s | 4,521 s | −13 s |
| 4.29 h | 5,441 s | 5,456 s | −15 s |
| 6.64 h | 6,774 s | 6,794 s | −20 s |
| 9.29 h | 8,550 s | 8,574 s | −24 s |
| 12.02 h | 10,526 s | 10,554 s | −28 s |
| 13.18 h | 12,115 s | 12,153 s | −38 s |

Across all 2,230 traces recorded after the first suspend, the median absolute difference between the two is 19.5 seconds, over a window where the skew itself grew to more than three hours.

The `quote` service lost precisely the time my laptop spent asleep.

## Why only that one service

Ten services, and only one of them has this problem.
The demo is deliberately polyglot, so it is easy to check what makes the odd one out odd.

| service | language | SDK |
| --- | --- | --- |
| `quote` | PHP | 1.15.0 |
| `cart` | .NET | 1.17.0 |
| `checkout` | Go | 1.45.0 |
| `frontend` | Node.js | 2.10.0 |
| `shipping` | Rust | 0.32.1 |

`quote` is the only PHP service in the demo, and it is the only service that drifted.

The mechanism is a reasonable design decision meeting an unreasonable situation.
A monotonic clock is one that only ever moves forward, at a steady rate, immune to NTP corrections and daylight saving and the user changing the date.
That is exactly what you want for measuring how long something took, which is most of what a tracing SDK does.
The catch is that a monotonic clock also does not advance while the machine is suspended, because from the monotonic clock's point of view no time passed — that is the guarantee, not a bug in it.

So if an SDK reads the wall clock once when the process starts and then derives every subsequent timestamp by adding monotonic elapsed time to that anchor, it will be correct forever on a machine that never sleeps, and it will fall behind by exactly the suspend duration on a machine that does.
The error is permanent and cumulative, because the anchor is never re-read.

Every other SDK here asks the operating system what time it is when a span starts.
The host clock gets corrected on wake, so those services simply resume telling the truth, with no memory of the gap and no need for one.

## What I actually take from this

The viewer was right the whole time.
It faithfully rendered timestamps that really were three hours apart, and my instinct to blame my own query layer cost me a while.

But there is a more uncomfortable observation sitting underneath, which is that a single misbehaving clock makes a trace unreadable in any waterfall UI, mine included.
When one service is stranded three hours from the rest, the time axis spans three hours, and the actual work — the part with all the spans in it, the part you opened the trace to look at — compresses into a strip a couple of pixels wide at the far edge.
Every span is present and every timestamp is accurate and the view is useless.

Jaeger does the same thing, for the same reason, so I do not think this is a mistake I made specifically.
It is more that "render spans on a linear time axis" quietly assumes the clocks agree, and nothing in the UI notices when that assumption fails.
Detecting the two-cluster shape and offering to break the axis, or just telling the reader that one service's clock disagrees with the others by three hours, would turn an unreadable trace into a diagnosis.

I have not built that yet.
But I did not expect the overnight capture to hand me the clock-skew test case I had been meaning to construct by hand, and it is a better one than I would have written, because I would not have thought to make the skew grow all night.

## A note on the numbers

Everything above comes from one capture: the OpenTelemetry Demo running under Docker on a 14-core Apple Silicon MacBook, exported over OTLP into a local otel-desktop-viewer store, 2,320,769 spans across 434,198 traces and 1.6 GB on disk.

Skew per trace is the minimum start time of the non-`quote` spans minus the minimum start time of the `quote` spans, across every trace holding both and at least twenty spans.

Sleep durations come from each `Sleep` entry's own duration field in `pmset -g log`.
It is worth saying that I got this wrong the first time by pairing `Sleep` events with the next `Wake` event, which produced a total of 31 seconds and briefly convinced me the whole theory was dead.
The pairing is wrong because a `Sleep` line is frequently followed by a `DarkWake` a second or two later, and the machine goes back to sleep immediately afterward.
The duration field on the line is the number to read.
