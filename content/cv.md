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

I'm a software engineer who builds developer tools.
The current one is otel-desktop-viewer, which shows you your OpenTelemetry data in a browser without standing up a backend.
I've worked the whole stack, embedded to observability.
I like building genuinely useful things, and the problems never stop being interesting.

## Skills

**Languages:** Go, TypeScript, JavaScript, SQL\
**Telemetry:** OpenTelemetry, OTLP, distributed tracing, observability\
**APIs:** gRPC, REST, JSON-RPC\
**Data:** DuckDB, PostgreSQL, analytical schema design\
**Frontend & tooling:** Svelte, React, CodeMirror/Lezer query language design, CI/CD

## Experience

### Open Source Maintainer, otel-desktop-viewer — 2023–present

[github.com/CtrlSpice/otel-desktop-viewer](https://github.com/CtrlSpice/otel-desktop-viewer)

{{< repo-stats repo="CtrlSpice/otel-desktop-viewer" stars="977" forks="45" downloads="4479" >}}, plus installs through Homebrew, Docker, and `go install`.

- Architected it as a custom OpenTelemetry Collector distribution: an exporter that writes telemetry, and a collector extension that owns the DuckDB store and the web UI, so storage outlives the pipeline
- Designed the analytical schema for traces, metrics, and logs, including a content-addressed attribute dictionary that dedupes attributes at ingest
- Implemented metric reduction and OTLP exponential-histogram merging in SQL, where a wrong answer still renders as a plausible chart — found four such bugs, and now verify the results against an independent implementation over randomized inputs
- Rewrote the frontend in Svelte 5 and TypeScript, replacing the original React implementation, and built a search language with a Lezer grammar in CodeMirror that the Go backend compiles into SQL
- Built the CI/CD release pipeline: a GitHub Actions matrix on native runners per platform (CGO rules out cross-compilation), publishing to Homebrew, GHCR, deb/rpm, and GitHub Releases across macOS, Linux, and Windows

### Senior Software Engineer, Telus — 2023–2025

Internal platform organization; deployment automation for services running on Kubernetes.

- Designed and implemented the tick-based state machine at the core of a tool automating blue-green and canary deployments
- Built a golden-signal metrics comparator used as a deploy gate, so only artifacts with stable metrics promote to production
- Drove OpenTelemetry adoption across teams: onboarded engineers new to OTel, paired with them on instrumenting their services, and closed instrumentation gaps in shared infrastructure — usually trace headers stripped in transit
- Technologies: Go, Kubernetes, client-go, PostgreSQL

### Career break — 2016–2022

### Firmware Testing and Integration Developer, Ciena — 2014–2016

- Wrote white- and black-box test suites for firmware on fiber-optic network equipment, built network simulation tooling to reproduce configuration errors, and thermal-cycled switches in environmental chambers to see how they'd do in a desert
- Technologies: C, C++, TCL, Perl, Bash

### Developer, Applied Research and Innovation, Algonquin College — 2012–2014

- Prototyped pro-athlete vision-training drills as a Unity game with an optometrist client, to see how they could be made accessible to more people; designed a backend-as-a-service for rapid prototyping
- Technologies: C#, Unity, Java, Android, MySQL

## Education

- [Recurse Center](https://www.recurse.com/) (remote) — 2025–2026\
  Self-directed programming retreat, admitted by application.
Completed 18 weeks across two batches.
- Engineering Technology and Computer Science, Algonquin College — 2014
