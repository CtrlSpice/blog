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

Things I've done OpenTelemetry to recently:

* latexmk, a decades-old Perl script, as the punchline for a terrible joke
* [Formula 1](https://github.com/CtrlSpice/bargeboard)

  <blockquote class="bluesky-embed" data-bluesky-uri="at://did:plc:p64asyewadpmz2dvdic52hz5/app.bsky.feed.post/3mmrin6c3rk2e"> <p>🧠: You should combine two hobbies.<br><br>Me: Alright. I'm listening.<br><br>🧠: You should overengineer the entire fuck out it!<br><br>Me: Obviously I'll overengineer the fuck out of it. What were you thinking? Knitting and mending?<br><br>🧠: F1 and OpenTelemetry<br><br>Me: Why are you like this?<br><br>🧠: 🫸🔭❤️🏎️🫷NOW KISS!!!</p> &mdash; CtrlSpice (they/them) (<a href="https://bsky.app/profile/ctrlspice.bsky.social/post/3mmrin6c3rk2e">@ctrlspice.bsky.social</a>) </blockquote> <script async src="https://embed.bsky.app/static/embed.js" charset="utf-8"></script>

* My own telemetry viewer, [which can now watch itself](https://github.com/CtrlSpice/otel-desktop-viewer/pull/228)
* My cargo e-bike is next. It has been warned.

You can find me on GitHub as [CtrlSpice](https://github.com/CtrlSpice), or email me at [amelia.ardath@gmail.com](mailto:amelia.ardath@gmail.com).
