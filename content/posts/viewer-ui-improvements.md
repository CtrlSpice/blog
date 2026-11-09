+++
date = '2026-11-08'
draft = true
title = 'UI improvements in otel-desktop-viewer'
type = 'page'
url = '/viewer-ui-improvements/'
+++

## Opening

- Follow-up to [the DuckDB rebuild post](/rebuilding-otel-desktop-viewer/), covering visible improvements since [0.5.0](https://github.com/CtrlSpice/otel-desktop-viewer/releases/tag/v0.5.0), through [0.7.0](https://github.com/CtrlSpice/otel-desktop-viewer/releases/tag/v0.7.0).
- A short introduction, then screenshots with one or two caption sentences each.
  Give the theme change a little more room.
- Publish before the agent-support post.
  Final title and count follow the selected screenshots; ten is a loose target.

## Search inside attributes

- Search nested map fields and individual array elements across traces, metrics and logs.
  [Nested-field search tests](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/desktopexporter/internal/store/search_nested_test.go).
- Show a query targeting one inner field, with the matching value visible in Details.
  Use the same example in the following section so readers can follow from search to inspection.
- Visual: the query, filtered results and expanded attribute together.

## Structured attributes and OTLP JSON

- [Structured values](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/desktopexporter/internal/frontend/src/components/shared/DetailProperty.svelte) get indentation, syntax highlighting and folding.
  Expand maps and arrays to follow their structure and collapse the parts that aren't relevant.
- JSON stored in a string can also get a formatted display, while Copy retains the original value.
  [Display and Copy behaviour](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/desktopexporter/internal/frontend/src/themes/README.md#typography-and-data-fidelity).
- Visual: a nested attribute with one object expanded and a neighbouring array collapsed, large enough to read the keys and values.
- Switch between Properties and OTLP JSON for the selected record.
  [Telemetry inspectors](https://github.com/CtrlSpice/otel-desktop-viewer/commit/5f9b1e12).
- Fold the JSON to focus on the relevant resource, scope or record fields.
  Copy and Download provide ways to take the data out of the inspector.
- Visual: the same record in Properties and JSON, with the view controls and record actions visible.

## Heatmaps and histograms

- Redesigned charts, count legends and percentile guides.
  The [heatmap palette](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/desktopexporter/internal/frontend/src/themes/README.md#graph-palettes) runs through violet, berry and warm rose, with a lightness progression that follows the counts.
- [Histogram contribution highlighting](https://github.com/CtrlSpice/otel-desktop-viewer/commit/9d7c64e5) uses texture to show how the selected source contributes to the distribution.
- Main visual: a large heatmap with a clear distribution, count legend and percentile guides.
  Give it enough room to show off.
- Second visual: a histogram with a source contribution selected and its texture visible.

## Compare metric series

- A [sortable series table](https://github.com/CtrlSpice/otel-desktop-viewer/commit/17fca430) uses the attributes that distinguish the series as columns.
  Hovering over a series highlights its chart line.
- Inspect a series, choose which lines to display, and adjust individual colours.
  The [inspection and visibility controls](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/desktopexporter/internal/frontend/src/themes/README.md#graph-palettes) work independently.
- Visual: a multi-series chart beside its table, with one line highlighted and its identifying attributes visible.

## Show the reporting gaps

- [Gauge charts preserve reporting gaps](https://github.com/CtrlSpice/otel-desktop-viewer/pull/621) instead of drawing a continuous line across missing data.
  The real datapoints on either side remain available to inspect.
- Visual: before and after using the same readings and time window, with the gap clearly visible.

## Choose your waterfall columns

- Add span fields and span, resource or scope attributes as [waterfall columns](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/desktopexporter/internal/frontend/src/components/traces/Waterfall/waterfall-columns.ts).
  Compare values across spans without opening each one.
- Visual: several spans with two useful attribute columns beside their duration bars.
  A small second image can show the column picker.

## Logs and events in the trace timeline

- [Span activity](https://github.com/CtrlSpice/otel-desktop-viewer/pull/541) brings correlated logs and span events into the waterfall.
  Nearby markers group together and open a popover for choosing a record.
- Follow a correlated log directly to its full record.
- Visual: a span with activity markers and an open popover, with the selected activity visible in Details.

## A table for logs

- [Paginated logs table](https://github.com/CtrlSpice/otel-desktop-viewer/commit/381f7064), with severity filters and error navigation.
  Choose and resize the columns needed for the investigation.
- Select a row to inspect the full record alongside the table.
- Visual: a filtered logs table with useful attribute columns and the selected log's details open.

## A service's telemetry at a glance

- [Home](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/desktopexporter/internal/frontend/src/components/home/README.md) shows the traces, metrics and logs belonging to a selected service.
  Signal tabs include matching-record counts and let you browse that service's telemetry.
- The service map provides the way in and shows recorded connections between services.
- Visual: one selected service, its signal counts and matching telemetry in Details.

## Theme

- Fought with Rosé Pine to keep labels and syntax readable across light and dark backgrounds, including hover and selection states.
  Adjusting foreground colours and background shades became repeated work.
- Switched to [Madoromi](https://github.com/the-moonwitch/madoromi/tree/f8bcc5aa1ec3f731fcba9471be7caa5df1ab5695), whose colours come from measurements of Pilot Iroshizuku fountain pen inks.
  Its [design method](https://github.com/the-moonwitch/madoromi/blob/f8bcc5aa1ec3f731fcba9471be7caa5df1ab5695/src/components/palettes/Method.tsx) balances readable contrast, similar apparent brightness and restrained saturation, with limits on colours that appear to glow.
- Adapted Madoromi for the viewer: quieter background tints, consistent colour roles, neutral text for data values, and contrast checks on normal, hover and selected backgrounds.
  Atkinson Hyperlegible Next and Mono provide the interface and code typography.
  [Viewer theme mappings and typography](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/desktopexporter/internal/frontend/src/themes/README.md).
- Four seasons, each with Day and Night variants.
  Service and series colours keep their assignments when the theme changes.
- Visual: the same detailed view in Day and Night, with labels, syntax and a selected row visible.

## Import and share telemetry

- [Drop OTLP JSON or JSONL files](https://github.com/CtrlSpice/otel-desktop-viewer/pull/592) anywhere in the viewer, or use the file picker.
- Download a trace, log or metric to share and reimport.
  [OTLP JSON exports](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/docs/otlp-json-export.md) include the retained resource and scope context.
- Visual: a short capture of dropping a telemetry file and opening an imported record, ending on its Download action.

## Other niceties

- [Navigation moved out of the left drawer and into the top bar](https://github.com/CtrlSpice/otel-desktop-viewer/blob/v0.7.0/desktopexporter/internal/frontend/src/components/shared/Toolbar/WorkspaceToolbar.svelte).
- [Scroll-to-top buttons](https://github.com/CtrlSpice/otel-desktop-viewer/commit/2a320e2f) in the logs table, log details and trace waterfall.
- [Copy buttons](https://github.com/CtrlSpice/otel-desktop-viewer/commit/2f50f963) for individual fields and whole records.
- [Resizable panes and columns](https://github.com/CtrlSpice/otel-desktop-viewer/pull/421).
- [Arrow-key navigation](https://github.com/CtrlSpice/otel-desktop-viewer/commit/5621aadc) between Details tabs.
- [Jump between log errors](https://github.com/CtrlSpice/otel-desktop-viewer/commit/dc334a6c), including across pages.
- [Both ends of long IDs stay visible](https://github.com/CtrlSpice/otel-desktop-viewer/commit/a7aab20a) in narrow waterfall columns.
- [Readable storage sizes](https://github.com/CtrlSpice/otel-desktop-viewer/commit/75f3c4f9), with exact byte counts on hover and Copy.

## Try it out

- Link to [installation instructions](https://github.com/CtrlSpice/otel-desktop-viewer#getting-started) and [0.7.0](https://github.com/CtrlSpice/otel-desktop-viewer/releases/tag/v0.7.0).
- Close with a brief mention that the agent-facing changes are next.
