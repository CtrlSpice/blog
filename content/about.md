+++
title = 'About'
# Keep the About page out of the home post list and off the post chrome:
# no date/reading-time meta, and no hire CTA (the page already ends with
# contact info).
hiddenInHomeList = true
hidemeta = true
hideCTA = true
+++

I'm Mila. I live in Squamish, BC, and I build observability tooling.

Most of my time goes into [otel-desktop-viewer](https://github.com/CtrlSpice/otel-desktop-viewer), a local OpenTelemetry viewer I've been maintaining since 2023. It takes OTLP over HTTP or gRPC and shows you your traces, metrics, and logs in a browser.

<!-- TODO: screenshot — trace waterfall, real (Bargeboard/F1) data, dark mode. Recapture at 2x: window at 1920 logical px => file must be 3840 wide. Then flatten RGBA -> RGB and `oxipng -o4 --strip all` (drops stray pHYs/eXIf). Add alt text (describe the view, for screen readers) + a caption that says what you're looking at, e.g. "a race weekend's telemetry, viewed locally". -->

On macOS it's a Homebrew cask; there are also release binaries, `.deb`/`.rpm` packages, Docker images, and `go install` — the [README](https://github.com/CtrlSpice/otel-desktop-viewer#getting-started) covers all of them, plus how to point your SDK at it.

Some cool things I've done for work:

* Built a tick-based state machine that resolves blue-green deployments based on production metrics
* Prototyped pro-athlete vision-training drills as a Unity game, to see how they could be made accessible to more people
* Thermal-cycled fiber-optic switches in environmental chambers to see how they'd do in a desert

You can find me on GitHub as [CtrlSpice](https://github.com/CtrlSpice), or email me at [amelia.ardath@gmail.com](mailto:amelia.ardath@gmail.com).
