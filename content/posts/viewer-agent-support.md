+++
date = '2026-10-13'
draft = true
hideCTA = true
title = 'Agent support in otel-desktop-viewer'
type = 'page'
url = '/viewer-agent-support/'
+++

I started building otel-desktop-viewer in 2023, and you might have noticed a few changes to the software development life cycle since then.
These days, you’re more likely to ask your agent why `checkout` is slow than look for bottlenecks on a dashboard.

Turns out, agents need feedback just as much as humans do, if not more.
Without being able to see what’s happening in your app, they’re flying blind.
You can save stdout to a file for your agent to read or grep, but querying OpenTelemetry data directly lets it follow connections between signals without reading through so much output.

otel-desktop-viewer already collects that telemetry in an embedded DuckDB database, keeping your traces, metrics and logs together in a local columnar store you can query with SQL.
I’ve extended otel-desktop-viewer to support coding agents, with new CLI tools they can call to investigate your application’s telemetry.

With the viewer running and receiving data, give your agent its URL and ask it to investigate.
For a closer look at the evidence, ask for an example trace and open it in the [viewer’s UI](/viewer-ui-improvements/).

Let’s look at the new CLI tools, how agents find them, and how I tested that they can use the viewer to investigate an incident.

## CLI tools and instructions

The CLI has commands for searching traces, logs and metrics, inspecting individual records, and querying the stored telemetry with SQL.
I want agents to get on with the investigation without reading the whole manual first, so the instructions come in two forms:

1. **Help**

   Your agent can start with the command list, then open a particular command’s `--help` for more detail.
   For a slow `checkout` request, `traces --help` includes example searches and explains which flags filter by service or change the time window.

   {{< figure src="/images/viewer-agent-support/traces-help.png" alt="Help for otel-desktop-viewer traces on a dark background. Examples show searching all traces, filtering to checkout over the last 30 minutes, and requesting JSON for an explicit time range. Flag descriptions include the defaults: the last hour, at most 25 summaries and http://localhost:8000." >}}

2. **Skill**

   The [skill](https://github.com/CtrlSpice/otel-desktop-viewer/blob/main/skills/otel-desktop-viewer/SKILL.md) helps your agent choose commands for an investigation:

   - Reusing your running viewer for the investigation and follow-up questions, and stopping only a viewer the agent started.
   - Matching questions to commands, using a lookup table.
   - Discovering, searching and inspecting telemetry, with examples.
   - Running SQL against the installed schema.
   - Interpreting results.

The CLI works for people, scripts and agents, using the same commands.
Agents already have shell and HTTP tools, so they can use these interfaces directly.
An MCP server would add another interface to configure and maintain; I wanted to test how well the existing ones supported an investigation first.

## Discoverability

Onboarding sets the tone for how a tool gets used.
With agents, this means chunking instructions into manageable pieces and unceremoniously shoving them anywhere one is likely to look.

Where they start depends on what you already have and what you ask them to do:

1. **Installing the viewer**

   The [repository’s README](https://github.com/CtrlSpice/otel-desktop-viewer) covers installation for people and agents alike, but the instructions for using the viewer differ.
   The [For agents](https://github.com/CtrlSpice/otel-desktop-viewer#-for-agents) section points to command help and explains how to read the skill through the CLI or `/llms.txt`.

2. **Instrumenting your service**

   Your agent can check telemetry in the viewer as they instrument your service.
   When they look up how to use the CLI, `--help` lists `skills` first; running that command prints the instructions.
   Individual commands have their own help with arguments, defaults and examples.

3. **Investigating telemetry in a running viewer**

   Once your telemetry is in the viewer, you can share its URL along with whatever you want investigated.
   A request to that URL returns links in both the headers and HTML to the same instructions at `/llms.txt` and the JSON-RPC API description at `/openrpc.json`.

4. **Using an installed skill**

   You can also install the [skill](https://github.com/CtrlSpice/otel-desktop-viewer/tree/main/skills/otel-desktop-viewer) in your coding agent.
   Its description helps them recognise tasks the viewer can help with and read the full instructions before they ever contact it.

The CLI and `/llms.txt` serve that same `SKILL.md`, bundled with the viewer.
I update the advice in one place, and both copies match the version you’re running.

{{< hire-cta show=true >}}

## Following an investigation

Checkout is slow and orders are failing in west; east is healthy.
You ask your agent to investigate.
The viewer’s [sample shop telemetry](https://github.com/CtrlSpice/otel-desktop-viewer/tree/main/testdata/otlp/small) gives us a handy example of that incident.

The [UI post](/viewer-ui-improvements/) showed how to inspect traces and logs in the browser; here, we follow the evidence through the CLI.

1. **Regional comparison**

   The first query compares requests by region before and after 08:00 UTC:

   | Region | Period | Failed / total | Mean duration |
   | --- | --- | ---: | ---: |
   | east | baseline | 0 / 8 | 200 ms |
   | east | incident | 0 / 8 | 200 ms |
   | west | baseline | 0 / 8 | 200 ms |
   | **west** | **incident** | **4 / 8** | **837.5 ms** |

2. **Payment-provider latency**

   East stays steady while west gets slower and starts failing, so the next step is to inspect a slow west request.
   In `checkout-019`, the payment-provider call takes 1250 ms, compared with 80 ms in healthy traces:

   ```sh
   otel-desktop-viewer trace \
     8190e585b2d994551a5b5fddb8e9d340
   ```

   | Operation | Duration |
   | --- | ---: |
   | POST /api/v2/orders | 1500 ms |
   | catalog/resolve-items | 20 ms |
   | inventory/reserve-stock | 20 ms |
   | order/create | 20 ms |
   | payment/charge | 1350 ms |
   | ↳ **stripe/create-charge** | **1250 ms** |

3. **Timeouts and circuit-breaker rejections**

   The provider call accounts for most of that request’s duration, but the failed requests need a closer look.
   In `checkout-020`, the provider times out after 2000 ms and checkout returns HTTP 504.
   The next two requests return 503 because the circuit breaker is open; their logs record `provider.attempted=false`, so neither reaches the provider.

   That explains three of the four failures.
   The fourth, `checkout-017`, is a card decline for insufficient funds (HTTP 402), a separate failure from the provider timeout.

   By `checkout-023/024`, provider calls are back to 80 ms and both requests return 200.

4. **Regional metrics**

   Those traces show individual requests slowing down, failing and recovering.
   The west-region histograms provide a cross-check over the same interval:

   - **Checkout:** 8 requests, 6.7 seconds total → 837.5 ms mean.
   - **Provider:** 6 attempts; the circuit breaker rejected the other 2 orders.

The answer includes trace IDs for follow-up in the viewer.
The underlying cause of the provider delay still needs investigation.

*[Working source: upgraded shared dataset on `test/release-installation-evals`; update the source link when that revision lands.]*

## Checking whether it works (evals in progress)

> “Under the most rigorously controlled conditions of pressure, temperature, volume, humidity, and other variables, the organism will do as it damn well pleases.”
>
> — Harvard Law of Animal Behaviour

I ran evals in fresh conversations and recorded the tool calls and answers.
The Evals view let me compare results and inspect individual runs.

**[App screenshot: Evals results overview.]**

1. **Installation**

   The request was “Install https://github.com/CtrlSpice/otel-desktop-viewer”.
   I checked that the executable used was v0.7.0.

   **[App screenshot: installation run with the command and version output expanded.]**

2. **Instruction discovery**

   Separate cases started with the CLI, a viewer URL or an installed skill.
   I checked how the instructions were found and used during the task.

3. **Investigation**

   I compared the checkout explanation with the sample’s known timings and failures.
   The recorded tool calls linked the answer to its supporting traces, logs and metrics.

   **[App screenshot: investigation answer, assessment and supporting tool result.]**

4. **Viewer processes**

   I checked that follow-up questions used the same viewer and that cleanup stopped only a process the agent had started.

*[Draft describes planned reruns. New cases, measured outcomes and app screenshots are still pending.]*

## What’s next

The Evals UI is experimental, and these runs will help me test it alongside the agent-facing tools.
I’ll cover how it compares results and connects answers to tool calls in a separate post.

## Try it out

[Install otel-desktop-viewer](https://github.com/CtrlSpice/otel-desktop-viewer#getting-started) and send it telemetry from your service.
Then give your agent the viewer’s URL and ask it to investigate.
The CLI and agent instructions are available in [v0.7.0](https://github.com/CtrlSpice/otel-desktop-viewer/releases/tag/v0.7.0).
