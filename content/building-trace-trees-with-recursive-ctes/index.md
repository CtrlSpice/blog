+++
date = '2025-09-01T08:39:50-07:00'
title = "Generating Trace Waterfalls with Recursive CTEs in DuckDB"
summary = "A recursive SQL walk with orphan promotion, depth-first sort paths, search annotations, and cycle recovery."
tags = ['OpenTelemetry', 'otel', 'otel-desktop-viewer', 'observability', 'distributed tracing', 'traces', 'trace waterfall', 'DuckDB', 'SQL', 'CTE']
author = 'Mila Ardath'
[cover]
  image = "cycle-recovery-waterfall.png"
  relative = true
  alt = "A recovered trace waterfall showing a healthy root subtree, an orphan promoted to the top level, warning triangles on salvaged spans, and a biohazard marker at the retained cycle cut."
  hiddenInSingle = true
  hiddenInList = true
+++

Let's talk about the last time I was accused of witchcraft:

{{< bluesky author="Jeremy Morrell" handle="@jeremymorrell.dev" profile="https://bsky.app/profile/jeremymorrell.dev" href="https://bsky.app/profile/jeremymorrell.dev/post/3lx3sy2nbv22v" date="August 23, 2025" datetime="2025-08-23T20:33:26.364Z" avatar="/building-trace-trees-with-recursive-ctes/jeremy-morrell.jpg" >}}
y'all [@ctrlspice.bsky.social](https://bsky.app/profile/ctrlspice.bsky.social) is doing some SQL dark magic with [@duckdb.org](https://bsky.app/profile/duckdb.org).
This builds and flattens a trace waterfall from the raw OpenTelemetry span data in one SQL query 🤯 (It even handles incomplete traces with orphan subtrees)
{{< /bluesky >}}

I think dark magic is a bit generous.
This is intermediate transmutation[^1] at best, by which I mean graph traversal.

A trace waterfall shows a request as nested operations over time.
We don't receive the data as a tree, though.
We get individual spans with IDs that describe their relationships, and have to construct the tree afterwards.

Let's do this in SQL and make it DuckDB's problem.

For one small trace, this is what we have, with the repeated trace ID shortened for display:

| name | `trace_id` | `span_id` | `parent_span_id` |
| --- | ---: | ---: | ---: |
| root | `...0439` | 1 | `null` |
| authenticate | `...0439` | 2 | 1 |
| checkout | `...0439` | 3 | 1 |
| fetch-user | `...0439` | 4 | 2 |

Stripped of payload fields, this is what we need:

```json
[
  { "name": "root", "depth": 0 },
  { "name": "authenticate", "depth": 1 },
  { "name": "fetch-user", "depth": 2 },
  { "name": "checkout", "depth": 1 }
]
```

By the end, DuckDB hands the front end the spans in the order it needs to render them:

{{< figure src="/building-trace-trees-with-recursive-ctes/healthy-search-context.png" alt="The healthy root trace rendered as a waterfall. Authenticate and checkout are children of root, fetch-user is nested beneath authenticate and highlighted with a Match label as the direct search result, and each row has a horizontal duration bar." caption="The healthy subtree preserves depth-first order while marking fetch-user as the direct search match." >}}

We'll build the query in stages: first the healthy tree, then search context, orphaned subtrees, and cycles.

## Start with the rows

The database identifies each span by `(trace_id, span_id)`.
The timing table below leaves out the trace ID they all share.
The database stores absolute timestamps, but here we're showing them as offsets from the start of the trace to make the timing easier to follow:

| name | `span_id` | `parent_span_id` | start offset |
| --- | ---: | ---: | ---: |
| root | 1 | `null` | 0 ms |
| authenticate | 2 | 1 | 100 ms |
| checkout | 3 | 1 | 120 ms |
| fetch-user | 4 | 2 | 150 ms |

It would be nice if we could `ORDER BY start_time` and call it a day.
Unfortunately, that leaves our tree looking like this:

```text
name                 span_id  parent_span_id  start offset
root                       1            null          0 ms
├── authenticate           2               1        100 ms
├── checkout               3               1        120 ms
│   └── fetch-user         4               2        150 ms
```

What we want is this:

```text
name                 span_id  parent_span_id  start offset
root                       1            null          0 ms
├── authenticate           2               1        100 ms
│   └── fetch-user         4               2        150 ms
└── checkout               3               1        120 ms
```

The [production spans table schema](https://github.com/CtrlSpice/otel-desktop-viewer/blob/ffd204444eb8ab3c7910e37073f42622f83aee69/desktopexporter/internal/store/queries/ddl/tables/spans.sql) is much wider, but we can simplify it to just four columns for this walkthrough:

```sql
create table spans (
    trace_id uuid,
    span_id ubigint not null,
    parent_span_id ubigint,
    start_time bigint,
    primary key (trace_id, span_id)
);
```

The composite key scopes each span ID to its trace.
`parent_span_id` is nullable because root spans ~~were Elves once, taken by the dark powers~~ don't have parents.
`start_time` can technically be `null`, but the ingest path always writes an integer.
If OTLP leaves the timestamp unset, it writes zero.

There is no foreign key on `parent_span_id`.
Children can arrive before their parents, and sometimes the parent never arrives at all.

The SQL blocks from here on are snippets.
You can see the full query [here](https://github.com/CtrlSpice/otel-desktop-viewer/blob/ffd204444eb8ab3c7910e37073f42622f83aee69/desktopexporter/internal/store/queries/spans/search_spans.sql).

## Prepare the walk

The caller gives us something we hope is a `trace_id` as `?`.
`search_params` tries to cast it once.
Then `trace_spans` pulls out that trace and leaves the payload columns behind.

```sql
search_params as (
    select try_cast(? as uuid) as trace_id
),

trace_spans as materialized (
    select s.trace_id, s.span_id, s.parent_span_id, s.start_time
    from spans s, search_params
    where s.trace_id = search_params.trace_id
)
```

Using `try_cast` means bad input becomes `null`, matches nothing, and we're done.
`materialized` makes sure the more expensive operations target a dataset bounded by the size of the trace, not the full dataset.

The relation still carries the shared `trace_id` and absolute `start_time` values.
To keep the example readable, the table omits the repeated trace ID and displays each timestamp as an offset from the trace's earliest span:

| `span_id` | `parent_span_id` | displayed start offset |
| ---: | ---: | ---: |
| 1 | `null` | 0 ms |
| 2 | 1 | 100 ms |
| 3 | 1 | 120 ms |
| 4 | 2 | 150 ms |

## Rank the rows

Before walking anything, the query assigns two positions.
`sibling_rank` records where a span sits among rows with the same parent.
`root_rank` gives every span a fixed position in the list, with spans that report no parent first.
Ideally we'd have just one of those, but incomplete traces are common in practice.
More on that later.

```sql
ranked as materialized (
    select t.*,
        row_number() over (
            partition by t.parent_span_id
            order by t.start_time, t.span_id
        ) as sibling_rank,
        row_number() over (
            order by
                case when t.parent_span_id is null then 0 else 1 end,
                t.start_time,
                t.span_id
        ) as root_rank
    from trace_spans t
)
```

Materializing `ranked` runs both windows once before the walk and lets every recursive level reuse their results.
For the running trace, those results are:

| name | `span_id` | `sibling_rank` | `root_rank` |
| --- | ---: | ---: | ---: |
| root | 1 | 1 | 1 |
| authenticate | 2 | 1 | 2 |
| checkout | 3 | 2 | 3 |
| fetch-user | 4 | 1 | 4 |

Only spans that become top-level rows (let's call them anchors) end up using `root_rank`.
Between `root_rank` and `sibling_rank`, we have all the information we need to build the tree!

## Walk the tree

The complete statement begins with `with recursive`.
Within it, `spans_tree` has two parts: the anchor member seeds top-level rows, and the recursive member repeatedly adds their children.
The production anchor also accepts a span whose reported parent is missing.
That second condition does not affect this trace, and we will return to it after the healthy path.

The walk also builds a `sort_path` for each span.
We'll talk about it in the next section.

```sql
spans_tree as (
    -- Anchor member: seed roots and spans whose parent is missing.
    select
        r.trace_id,
        r.span_id,
        r.parent_span_id,
        r.start_time,
        0 as depth,
        array[r.root_rank] as sort_path
    from ranked r
    where r.parent_span_id is null
       or r.parent_span_id not in (select span_id from trace_spans)

    union all

    -- Recursive member: add the children of the previous iteration.
    select
        r.trace_id,
        r.span_id,
        r.parent_span_id,
        r.start_time,
        st.depth + 1,
        st.sort_path || array[r.sibling_rank] as sort_path
    from ranked r
    join spans_tree st on r.parent_span_id = st.span_id
)
```

The anchor places `root` as a top-level row with a one-item path.
On each iteration, DuckDB reads the rows produced by the previous iteration, finds their children, and appends those rows to the result.
The walk stops when an iteration finds no more children.

Because `ranked` contains only the requested trace, the recursive self-join can match on `span_id` alone.
Later joins back to unrestricted tables use both `trace_id` and `span_id` to preserve that scope.

Ignoring output order for the moment, `spans_tree` has attached a depth and path to every row:

| name | `span_id` | `parent_span_id` | `depth` | `sort_path` |
| --- | ---: | ---: | ---: | --- |
| root | 1 | `null` | 0 | `[1]` |
| authenticate | 2 | 1 | 1 | `[1, 1]` |
| checkout | 3 | 1 | 1 | `[1, 2]` |
| fetch-user | 4 | 2 | 2 | `[1, 1, 1]` |

## Sort paths

Because start time alone cannot keep a subtree together, the query needs one value whose ordinary sort order produces depth-first traversal.
`sort_path` records the sibling choice made at each level from the root to a span: the root starts at `[1]`, its first child appends `1`, and that child's first child appends another `1`.
`checkout` is the root's second child, so its path is `[1, 2]` even though it started before `fetch-user`.

```text
root                 [1]
├── authenticate     [1, 1]
│   └── fetch-user   [1, 1, 1]
└── checkout         [1, 2]
```

[DuckDB compares lists lexicographically](https://duckdb.org/docs/current/sql/data_types/list.html#comparison-and-ordering).
Each prefix sorts before the longer paths below it, while sibling ranks keep neighbouring subtrees in start-time order.
That puts `[1, 1, 1]` before `[1, 2]`, producing the display order we wanted.

```sql
select trace_id, span_id, parent_span_id, start_time, depth
from spans_tree
order by sort_path;
```

When spans have the same `start_time`, we use `span_id` as a tie-breaker to keep their order deterministic.
The production query carries this structure into the payload and JSON stages below.

## Add the payload

The recursive rows carry only IDs, timing, depth, and `sort_path`, which keeps each row narrow while DuckDB copies it through the walk.
Once recursion finishes, `tree` joins the kitchen sink back in.

```sql
tree as materialized (
    select
        st.depth,
        st.sort_path,
        s.trace_id,
        s.span_id,
        s.parent_span_id,
        s.name,
        s.start_time,
        s.end_time,
        s.status_code,
        s.attribute_ids
    from spans_tree st
    join spans s
        on s.trace_id = st.trace_id
       and s.span_id = st.span_id
)
```

`tree` is the boundary between traversal and response shaping.
New response fields join here, after DuckDB has finished copying rows through recursion.

## Add search

The waterfall still needs every span when only a few match.
Removing non-matches would discard ancestor context and change the depths and paths the query builds.

For a simple name predicate, `matched` could be calculated directly in `tree`.
The production search is kept as a separate relation so matching cannot change the recursive input, and more involved predicates do not widen the walk.
For a name search, that relation has this shape:

```sql
matched_spans as (
    select s.span_id
    from spans s, search_params
    where s.trace_id = search_params.trace_id
      and s.name = ?
)
```

In the final projection, `tree` is aliased as `ts` and left joined to those IDs.

```sql
from tree ts
left join matched_spans ms on ts.span_id = ms.span_id
```

The presence of a match becomes the flag used by the interface.

```sql
'matched',
case when ms.span_id is not null then true else false end
```

The query returns the complete ordered trace and marks direct matches.
The front end can keep paths to matches open and collapse unrelated subtrees without changing the display topology.

Stripped of payload fields and wrapper objects, a search for `fetch-user` now produces rows shaped like these:

```json
[
  { "span_id": 1, "name": "root", "depth": 0, "matched": false },
  { "span_id": 2, "name": "authenticate", "depth": 1, "matched": false },
  { "span_id": 4, "name": "fetch-user", "depth": 2, "matched": true },
  { "span_id": 3, "name": "checkout", "depth": 1, "matched": false }
]
```

## Render the healthy trace

The final JSON macro turns absolute timestamps into the position and width the waterfall needs:

```sql
'start', ts.start_time - trace_start_ns,
'dur', ts.end_time - ts.start_time
```

DuckDB does the recursive work once and returns each span with its display order, depth, and timing already attached.
This has a number of benefits!

- The backend can stay simple.
  It just passes the result on to the frontend.
- The browser does not run another recursive traversal to decide order or depth.
  Expensive computation stays in the database, and we avoid jank.
- The browser uses the same ordered list for rendering, search reveal, and keyboard navigation.
- The browser's virtual list mounts only the rows in and around the viewport, rather than every span in the trace, which is how the interface stays snappy, even with thousands of spans (or at least snap-adjacent).

## When traces misbehave

In a complete, valid trace, every non-root span has one parent, and repeatedly following its reported `parent_span_id` eventually reaches a root.
However, otel-desktop-viewer is meant to be used in development, where we might receive data that's... not quite right.
We need to handle malformed telemetry gracefully and gently let the user know that they might want to fix it.

The screenshots below use the same healthy relationships, plus these rows:

| name | `span_id` | `parent_span_id` | start offset | condition |
| --- | ---: | ---: | ---: | --- |
| orphan-root | 5 | 255 (missing) | 50 ms | parent absent |
| orphan-child | 6 | 5 | 75 ms | child of the promoted orphan |
| early-off-cycle-child | 7 | 9 | 10 ms | descendant of the cyclic component |
| cycle-a | 9 | 8 | 500 ms | opens the cycle |
| cycle-b | 8 | 9 | 520 ms | closes the cycle |

### Orphans

The anchor condition we deferred earlier promotes a span whose reported parent is absent to a top-level row.
Promotion changes only its place in the display tree.
The stored `parent_span_id` remains faithful to the telemetry.
The normal walk then continues through its descendants.

`root_rank` is calculated before the anchor filter, so non-anchor spans still consume numbers.
In this fixture, `early-off-cycle-child` consumes rank 2 even though it is not an anchor, so `orphan-root` begins at `[3]`:

```text
orphan-root          [3]
└── orphan-child     [3, 1]
```

The gap does not change its order relative to the healthy root.
The primary key prevents `span_id` from being `null`, so the anchor's `not in` check is safe here.

### Cycles

Or maybe you vibe coded too close to the sun and your span is its own grandfather:

```text
cycle-a -> cycle-b -> cycle-a
```

Neither span offers the normal walk a top-level starting point, so it reaches neither.

The normal query reports that gap as `count(trace_spans) - count(tree)`, returned as a separate integer beside the trace JSON.
When the count is nonzero, the backend reruns the whole trace with a [salvage query](https://github.com/CtrlSpice/otel-desktop-viewer/blob/ffd204444eb8ab3c7910e37073f42622f83aee69/desktopexporter/internal/store/queries/spans/salvage_spans.sql#L75-L150) rather than merging a fragment into the first response.
If salvage itself fails, the backend returns the shorter normal result rather than replacing a usable partial waterfall with an error page.

Every unreached span gets an `entry_rank` from its `start_time, span_id` order, then seeds a candidate walk.
Each candidate tracks the IDs it has visited and stops before repeating one.
Because a span can appear in several candidates, deduplication keeps the placement with the lowest `entry_rank`, then the shallowest depth.

In this fixture, `early-off-cycle-child` starts first and keeps its `entry_rank` 1 placement as a separate top-level row.
Of the actual cycle entries, `cycle-a` ranks before `cycle-b`, so its candidate wins both spans:

```text
cycle-a  cyclePoint
└── cycle-b
```

The marker means that `cycle-a`'s reported parent appears below it in the retained candidate chain.
The earlier off-cycle child keeps its salvage-warning triangle but not the `cyclePoint` biohazard because its reported parent does not appear in its one-row chain.

Carrying ancestry and a complete relative path makes each recursive row wider, so that work stays in the fallback and runs only when the normal query leaves spans behind.

{{< figure src="/building-trace-trees-with-recursive-ctes/search-context.png" alt="A trace waterfall filtered to fetch-user. The matching fetch-user row is highlighted and labelled Match beneath root and authenticate, while the descendants of unrelated orphan and cycle branches are collapsed." caption="A search for fetch-user keeps its path open and folds unrelated subtrees." >}}

{{< figure src="/building-trace-trees-with-recursive-ctes/cycle-recovery-waterfall.png" alt="A recovered trace waterfall showing the healthy root subtree, an orphan promoted to the top level, warning triangles on early-off-cycle-child and cycle-b, and a biohazard cycle-point marker on the selected cycle-a row." caption="Warning triangles mark salvaged rows. The biohazard marks the retained cycle cut at cycle-a." >}}

{{< figure src="/building-trace-trees-with-recursive-ctes/cycle-recovery-detail.png" alt="The detail panel for cycle-a shows its span ID as 9 and its reported parent span ID as 8, which belongs to cycle-b below it in the recovered waterfall." caption="The detail panel preserves the reported parent and span IDs behind the cycle annotation." >}}

## Whew

That was a lot, but we did it!
The [full query](https://github.com/CtrlSpice/otel-desktop-viewer/blob/ffd204444eb8ab3c7910e37073f42622f83aee69/desktopexporter/internal/store/queries/spans/search_spans.sql) handles the healthy path, search context, orphans, and cycles, then hands the front end one ordered list.
We have successfully implemented my favourite guiding principle lately: when you can, make it DuckDB's problem.

[^1]: I'm not allowed near Evocation or [Technomancy](https://strangehorizons.com/wordpress/non-fiction/articles/installing-linux-on-a-dead-badger-users-notes/) since incident [REDACTED].
