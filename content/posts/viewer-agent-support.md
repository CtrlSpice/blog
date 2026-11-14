+++
date = '2026-10-13'
draft = true
title = 'Agent support in otel-desktop-viewer'
type = 'page'
url = '/viewer-agent-support/'
+++

So agents, amirite?
They’ve fundamentally changed how we write, instrument, and observe software, including how much of that work we choose to do ourselves.
These days, you’re more likely to ask your agent why `checkout` is slow than look for bottlenecks on a dashboard.

Depending on the answer, you may want a closer look at the trace waterfall.
Or maybe you haven’t opened the UI in 6-7 months and don’t really see that changing.
That’s okay too.
Seeing (and [poking at](/viewer-ui-improvements/)) your own data is a great way to deepen your understanding, but it’s not the only one.

Both workflows deserve the same care.
Your agent needs instructions it can find and summaries it can follow up on.
Here’s what that looks like in otel-desktop-viewer today.

## Discoverability

> “I am Bender. Please insert girder.”
>
> — Bender, *Futurama*, “Bendless Love” (Season 3, Episode 6)

Onboarding sets the tone for how a tool gets used.
With agents, this means chunking instructions into manageable pieces (more on this later) and unceremoniously shoving them anywhere one is likely to look.

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

## Instructions for agents

> “You want me to do two things?”
>
> — Bender, *Futurama*, “Benderama”

Once they’ve found the instructions, I want them to get on with the investigation without reading the whole manual first.

### Help

Your agent can start with the command list, then open a particular command’s `--help` for more detail.
For a slow `checkout` request, `traces --help` includes example searches and explains which flags filter by service or change the time window.

{{< figure src="/images/viewer-agent-support/traces-help.png" alt="Help for otel-desktop-viewer traces on a dark background. Examples show searching all traces, filtering to checkout over the last 30 minutes, and requesting JSON for an explicit time range. Flag descriptions include the defaults: the last hour, at most 25 summaries and http://localhost:8000." >}}

### Skill

The [skill](https://github.com/CtrlSpice/otel-desktop-viewer/blob/main/skills/otel-desktop-viewer/SKILL.md) helps your agent choose commands for an investigation:

- Connecting to one viewer and keeping that session.
- Matching questions to commands, using a lookup table.
- Discovering, searching and inspecting telemetry, with examples.
- Running SQL against the installed schema.
- Interpreting results and handling empty results or errors.

## Following an investigation

## Session management

## Evals

## Try it out
