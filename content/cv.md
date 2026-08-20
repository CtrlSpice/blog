+++
title = 'Mila Ardath'
layout = 'cv'
# Not a post: keep it off the home list and the post chrome.
hiddenInHomeList = true
hidemeta = true
hideCTA = true
# Rendered in the page header, under the name.
contact = 'Squamish, BC, Canada · [amelia.ardath@gmail.com](mailto:amelia.ardath@gmail.com) · [github.com/CtrlSpice](https://github.com/CtrlSpice)'
+++

I'm a software engineer who builds developer tools. The current one is otel-desktop-viewer, which shows you your OpenTelemetry data in a browser without standing up a backend. I've worked the whole stack, embedded to observability. I like building genuinely useful things, and the problems never stop being interesting.

## Skills

Go, TypeScript, SQL, DuckDB, PostgreSQL, schema design, Svelte, OpenTelemetry, OTLP, gRPC, distributed tracing, observability, query language design (Lezer/CodeMirror), GitHub Actions

## Experience

### Open Source Maintainer, otel-desktop-viewer — 2023–present

[github.com/CtrlSpice/otel-desktop-viewer](https://github.com/CtrlSpice/otel-desktop-viewer)

A local OpenTelemetry viewer: receives OTLP over HTTP and gRPC and renders traces, metrics, and logs in a browser, with no external observability platform required.

- Architected it as a custom OpenTelemetry Collector distribution: an exporter that writes telemetry, and a collector extension that owns the DuckDB store and the web UI, so storage outlives the pipeline
- Designed the analytical schema for traces, metrics, and logs, including a content-addressed attribute dictionary that dedupes attributes at ingest
- Implemented metric reduction and OTLP exponential-histogram merging in SQL, where a wrong answer still renders as a plausible chart — found four such bugs, and now verify the results against an independent implementation over randomized inputs
- Built the Svelte 5 frontend, including a search language with a Lezer grammar in CodeMirror that the Go backend compiles into SQL
- Built the release pipeline: a GitHub Actions matrix on native runners per platform (CGO rules out cross-compilation), publishing to Homebrew, GHCR, deb/rpm, and GitHub Releases across macOS, Linux, and Windows

### Senior Software Engineer, Telus — 2023–2025

Internal platform organization; deployment automation for services running on Kubernetes.

- Designed and implemented the tick-based state machine at the core of a tool automating blue-green and canary deployments
- Built a golden-signal metrics comparator used as a deploy gate, so only artifacts with stable metrics promote to production
- Authored an OpenTelemetry adoption RFC and presented it to the Architecture Guild
- Technologies: Go, Kubernetes, client-go, PostgreSQL

### Career break — 2016–2022

### Firmware Testing and Integration Developer, Ciena — 2014–2016

- White-box firmware testing on fiber-optic network equipment, network simulation tooling to reproduce configuration errors, and thermal cycling in environmental chambers to see how switches would do in a desert
- Technologies: C, C++, TCL, Perl, Bash

### Developer, Applied Research and Innovation, Algonquin College — 2012–2014

- Prototyped pro-athlete vision-training drills as a Unity game with an optometrist client, to see how they could be made accessible to more people; designed a backend-as-a-service for rapid prototyping
- Technologies: C#, Unity, Java, Android, MySQL

## Education

- [Recurse Center](https://www.recurse.com/) (remote) — 2025–2026\
  Self-directed programming retreat, admitted by application. Completed 18 weeks across two batches.
- Engineering Technology and Computer Science, Algonquin College — 2014
