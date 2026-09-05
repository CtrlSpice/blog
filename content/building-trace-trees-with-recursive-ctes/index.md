+++
date = '2025-09-01T08:39:50-07:00'
draft = true
title = "Generating Trace Waterfalls with Recursive CTEs in DuckDB"
description = "DuckDB returns the trace waterfall ordered, annotated, and ready to render."
summary = "A depth-first SQL walk with orphan promotion, start-time sibling ordering, search annotations, and cycle recovery."
tags = ['OpenTelemetry', 'otel', 'otel-desktop-viewer', 'observability', 'distributed tracing', 'traces', 'trace waterfall', 'DuckDB', 'SQL', 'CTE']
author = 'Mila Ardath'
+++

In [`otel-desktop-viewer`](https://github.com/CtrlSpice/otel-desktop-viewer), I shape DuckDB query results around how the interface will use them.
For the waterfall, that means returning spans in depth-first order, along with each span's depth and whether it matched the current search.
Keeping the topology in DuckDB avoids another implementation in Go or the browser and keeps filtering, ordering, and projection together.
The query pipeline does most of the structural work: it preserves spans with missing parents, orders siblings, recovers cycles, and keeps unrelated traces out of the walk.
If I've done my job well, the interface stays snappy (or at least snap-adjacent) even when the database is full of large traces.

## The target order

Given this synthetic trace, with start offsets from its earliest span:

```text
root                      0 ms
├── authenticate        100 ms
│   └── fetch-user      150 ms
└── checkout            400 ms
```

The query should return:

| span | start offset | depth | `sort_path` |
| --- | ---: | ---: | --- |
| root | 0 ms | 0 | `[1]` |
| authenticate | 100 ms | 1 | `[1, 1]` |
| fetch-user | 150 ms | 2 | `[1, 1, 1]` |
| checkout | 400 ms | 1 | `[1, 2]` |

`authenticate` starts before `checkout`, so it is the first child of `root`.
Each child appends its sibling number to its parent's path.
Sorting those paths puts `fetch-user` directly after `authenticate`, then moves on to `checkout`.

## The span table

The recursive walk uses four columns from the production `spans` table:

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
`parent_span_id` is nullable because root spans ~~were Elves once, taken by the dark powers, tortured and mutilated. A ruined and terrible form of life.~~ don't have parents.
A foreign key would make ingestion brittle: children can arrive before their parents, and a partial capture may omit the parent entirely.
The walk leaves the remaining payload fields behind until recursion finishes.

## Prepare the walk

The normal path begins by naming the requested trace and isolating the four columns used by the walk.

```sql
with recursive
search_params as (
    select try_cast(? as uuid) as trace_id
),

trace_spans as materialized (
    select s.trace_id, s.span_id, s.parent_span_id, s.start_time
    from spans s, search_params
    where s.trace_id = search_params.trace_id
)
```

`search_params` turns the caller's argument into a UUID once, then gives the rest of the query a named column to reuse.
Using `try_cast` means bad input becomes `null` and cleanly matches no trace.

Materializing `trace_spans` gives every recursive step the same trace-sized relation to join.
Without it, the database would inline the CTE and expand each reference back into a full-table scan.

## Rank the forest

The waterfall renders a tree as a flat list of indented rows.
Depth supplies the indentation, while depth-first display order requires a route to each span through the sibling positions above it.
Before the walk begins, `ranked` calculates the positions that make up those routes.

```sql
ranked as materialized (
    select t.*,
        row_number() over (
            partition by t.parent_span_id
            order by t.start_time
        ) as sibling_rank,
        row_number() over (
            order by
                case when t.parent_span_id is null then 0 else 1 end,
                t.start_time
        ) as root_rank
    from trace_spans t
)
```

`sibling_rank` orders the spans that share a parent by start time.
The walk can also start from several anchors that do not necessarily share a parent, so `root_rank` gives the whole forest one order.
True roots come before promoted orphans, and each group follows start-time order.

Materializing `ranked` runs both window functions once before the walk, then lets every depth reuse their ranks.

## Recursion

`spans_tree` turns those ranked rows into the display forest.

```sql
spans_tree as (
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

The first `select`, above `union all`, is the anchor member.
It seeds the result with true roots and spans whose reported parent is absent from the capture.
`not in` is safe here because the primary key prevents `span_id` from being `null`.
Both kinds of anchor enter the walk at depth zero.

On each iteration, the recursive member below `union all` reads the rows produced by the previous iteration and finds their children.
DuckDB adds those rows to the accumulated result, then exposes them as the next iteration's input.
The recursion stops when an iteration finds no more children.

Because `ranked` contains only the requested trace, the recursive self-join can match on `span_id` alone.
Later joins back to unrestricted tables use both `trace_id` and `span_id` to preserve that scope.

`union all` does not create duplicate placements on the normal path because each stored span has one parent, giving every acyclic span one route from an anchor.
A cyclic component has no anchor, so this walk omits it rather than looping; the fallback path handles it later.

## Sort paths

`array[...]` starts each anchor with a one-item `sort_path`, and `||` appends a child's `sibling_rank` to its parent's path.

```text
root                 [1]
├── authenticate     [1, 1]
│   └── fetch-user   [1, 1, 1]
└── checkout         [1, 2]

orphan               [5]
└── orphan-child     [5, 1]
```

DuckDB's list ordering places each prefix before the longer paths below it, while the accumulated ranks keep neighbouring subtrees in start-time order.
`root_rank` is calculated before the anchor filter runs, so non-anchor spans still consume numbers.
That is why the orphan above starts at `[5]`; the gap does not change its position relative to the root.
Tied timestamps remain unstable; adding `span_id` after `start_time` in both windows would make them deterministic.

The final query orders the accumulated rows by their paths.

```sql
select trace_id, span_id, parent_span_id, start_time, depth
from spans_tree
order by sort_path;
```

That produces the depth-first preorder shown in the target table.
The [complete production query](https://github.com/CtrlSpice/otel-desktop-viewer/blob/main/desktopexporter/internal/store/queries/spans/search_spans.sql) carries this structure into the payload and JSON stages described below.

## After the walk

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

## Search

The waterfall still needs every span in the selected trace when only a few match, because removing rows would discard ancestor context and change depth and sort paths.
A separate CTE finds the matching span IDs without touching the recursive input.
For a name search, that CTE has this shape:

```sql
matched_spans as (
    select s.span_id
    from spans s, search_params
    where s.trace_id = search_params.trace_id
      and s.name = ?
)
```

Once the walk finishes, `tree` is left joined to the matches.

```sql
left join matched_spans ms on ts.span_id = ms.span_id
```

The final JSON projection turns the presence of a match into the flag used by the interface.

```sql
'matched',
case when ms.span_id is not null then true else false end
```

The query returns the complete ordered trace and marks direct matches.
The front end uses that structure to keep paths to matches open and collapse unrelated subtrees.

{{< figure src="/building-trace-trees-with-recursive-ctes/search-context.png" alt="A trace waterfall filtered to fetch-user. The matching fetch-user row is highlighted beneath root and authenticate, while the descendants of unrelated orphan and cycle branches are collapsed." caption="A search for fetch-user keeps its path open and folds unrelated subtrees." >}}

## Orphans

A capture can be missing a parent because a batch was dropped or the data was trimmed.
The query keeps the reported parent ID intact.
Promotion changes only the span's place in the display tree: it receives depth zero, and the normal walk continues through its descendants.
The result preserves the incomplete parent link and gives the surviving subtree a navigable root.

## Cycles

A cycle gives the walk no such entry point.

```text
span-a parent = span-b
span-b parent = span-a
```

Because a span has only one parent, a cyclic component cannot also lead back to a root.
The normal walk never reaches either span above.

The normal query reports the gap as `count(trace_spans) - count(tree)`, returned as a separate integer beside the trace JSON.
When that count is nonzero, the backend reruns the whole trace with a salvage query instead of merging a recovered fragment into the first result.

The recovery core starts a candidate walk from every unreached span, tracks the IDs visited along each path, and keeps one placement for each span.

```sql
salvage_seed as materialized (
    select r.*,
        row_number() over (
            order by r.start_time, r.span_id
        ) as entry_rank
    from ranked r
    where r.span_id not in (select span_id from spans_tree)
),

salvage_walk as (
    select
        sd.span_id, sd.parent_span_id, sd.trace_id, sd.start_time,
        sd.entry_rank, 0 as depth, [sd.span_id] as visited
    from salvage_seed sd

    union all

    select
        r.span_id, r.parent_span_id, r.trace_id, r.start_time,
        sw.entry_rank, sw.depth + 1,
        list_append(sw.visited, r.span_id)
    from salvage_seed r
    join salvage_walk sw on r.parent_span_id = sw.span_id
    where list_position(sw.visited, r.span_id) is null
),

salvaged as materialized (
    select span_id, parent_span_id, trace_id, start_time, depth, entry_rank
    from salvage_walk
    qualify row_number() over (
        partition by span_id
        order by entry_rank, depth
    ) = 1
)
```

`visited` stops a candidate walk before it follows a parent link around the cycle again.
Because several seeds can reach the same span, the final window keeps the placement reached from the earliest entry.

For the two-span example, suppose `span-a` sorts first in `salvage_seed`.
Its candidate walk produces the placement that survives deduplication:

| span | `entry_rank` | depth | `salvaged` | `cyclePoint` |
| --- | ---: | ---: | --- | --- |
| span-a | 1 | 0 | `true` | `true` |
| span-b | 1 | 1 | `true` | `false` |

When the production query appends those rows to the normal walk, this condition identifies the cycle point:

```sql
sv.depth = 0 and exists (
    select 1 from salvaged p
    where p.entry_rank = sv.entry_rank
      and p.span_id = sv.parent_span_id
)
```

The `entry_rank` comparison requires the display root and its reported parent to come from the same candidate walk.
An earlier off-cycle descendant can become a separate display root, but its parent belongs to another recovered chain and does not satisfy the condition.
Here, `span-a` is the display root and its parent, `span-b`, appears below it in the same chain, so that parent link closes the loop and `span-a` carries `cyclePoint`.

Carrying ancestry makes each recursive row wider, so that work stays in the fallback and runs only when the normal query leaves spans behind.

{{< figure src="/building-trace-trees-with-recursive-ctes/cycle-recovery.png" alt="A synthetic trace waterfall with a normal root tree, an orphan promoted to depth zero, an early off-cycle child, and a recovered two-span cycle. The selected cycle-a row has a biohazard marker, and its detail panel explains that its parent points into its own subtree." caption="An orphan, an early off-cycle child, and a recovered two-span cycle in one synthetic trace." >}}

## Rendering

By the time the response reaches the front end, DuckDB has fixed the vertical display order and attached a depth to each span.
It also sends each span's start offset and duration, which the front end converts into horizontal positions and widths.
Interactions still need a local view of the topology, so the front end derives structural maps for collapsing, search reveal, and keyboard navigation from row order and depth.
It cannot safely trust `parent_span_id`, which may name a missing parent or close a cycle.
