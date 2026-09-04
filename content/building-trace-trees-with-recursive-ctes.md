+++
date = '2025-09-01T08:39:50-07:00'
draft = true
title = "Generating Trace Waterfalls with Recursive CTEs in DuckDB"
description = "How otel-desktop-viewer uses a recursive CTE in DuckDB to build trace waterfalls."
summary = "A depth-first SQL walk that handles missing parents, sibling order, search matches, and cycles."
tags = ['OpenTelemetry', 'otel', 'otel-desktop-viewer', 'observability', 'distributed tracing', 'traces', 'trace waterfall', 'DuckDB', 'SQL', 'CTE']
author = 'Mila Ardath'
+++

In [`otel-desktop-viewer`](https://github.com/CtrlSpice/otel-desktop-viewer), I shape DuckDB query results around how the interface will use them.
For the waterfall, that means returning spans in depth-first order, along with each span's depth and whether it matched the current search.
Keeping the topology in DuckDB avoids another implementation in Go or the browser and keeps filtering, ordering, and projection together.
It also lets me make storage and computation tradeoffs around how people navigate the result.
The recursive CTE does the structural work: it preserves spans with missing parents, orders siblings, recovers cycles, and keeps unrelated traces out of the walk.
If I've done my job well, the interface stays snappy (or at least snap-adjacent) even when the database is full of large traces.

## The target order

Given this trace:

```text
root
├── authenticate
│   └── fetch-user
└── checkout
```

The query should return:

| span | depth |
| --- | ---: |
| root | 0 |
| authenticate | 1 |
| fetch-user | 2 |
| checkout | 1 |

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
Because a parent may arrive later or never appear in the capture, `parent_span_id` stays nullable and unconstrained.
The walk leaves the remaining payload fields behind and joins them back in after recursion.

The normal path reduces to this query.

```sql
with recursive
search_params as (
    select try_cast(? as uuid) as trace_id
),

trace_spans as materialized (
    select s.trace_id, s.span_id, s.parent_span_id, s.start_time
    from spans s, search_params
    where s.trace_id = search_params.trace_id
),

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
),

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

select trace_id, span_id, parent_span_id, start_time, depth
from spans_tree
order by sort_path;
```

`trace_spans` isolates one trace, `ranked` assigns its ordering keys, and `spans_tree` follows the parent-child relationships.
The recursion builds a sort key for each row; the final `order by sort_path` turns the accumulated rows into depth-first preorder.

`search_params` turns the caller's argument into a UUID once, then gives the rest of the query a named column to reuse.
Using `try_cast` means bad input becomes `null` and cleanly matches no trace.

## Recursion

The first `select` inside `spans_tree`, above `union all`, is the anchor member.
It seeds the result with roots and orphans.

On each iteration, the recursive member below `union all` reads the rows produced by the previous iteration and finds their children.
DuckDB adds those rows to the accumulated result, then exposes them as the next iteration's input.
The recursion stops when an iteration finds no more children.

A complete trace normally starts from a span with no parent.
The anchor member also starts from spans whose reported parent is absent from the capture.

```sql
where r.parent_span_id is null
or r.parent_span_id not in (select span_id from trace_spans)
```

`not in` is safe here because the primary key prevents `span_id` from being `null`.
Both kinds of anchor enter the walk at depth zero.

The other half of the CTE finds the children of those rows.

```sql
select
    r.trace_id,
    r.span_id,
    r.parent_span_id,
    r.start_time,
    st.depth + 1,
    st.sort_path || array[r.sibling_rank] as sort_path
from ranked r
join spans_tree st on r.parent_span_id = st.span_id
```

Because `ranked` contains only the requested trace, the recursive self-join can match on `span_id` alone.
Later joins back to unrestricted tables use both `trace_id` and `span_id` to preserve that scope.

`union all` does not create duplicate placements on the normal path because each stored span has one parent, giving every acyclic span one route from an anchor.
A cyclic component has no anchor, so this walk omits it rather than looping; the fallback path handles it later.

## Sort paths

The waterfall renders a tree as a flat list of indented rows.
Depth supplies the indentation, while depth-first display order requires a route to each span through the sibling positions above it.
`ranked` calculates those sibling positions from start time before the walk begins.

```sql
row_number() over (
    partition by t.parent_span_id
    order by t.start_time
) as sibling_rank
```

An anchor gets a one-item `sort_path`, and its children append their sibling rank.

```text
root                 [1]
├── authenticate     [1, 1]
│   └── fetch-user   [1, 1, 1]
└── checkout         [1, 2]

orphan               [5]
└── orphan-child     [5, 1]
```

DuckDB's list ordering places each prefix before the longer paths below it, while sibling ranks keep neighbouring subtrees in start-time order.
`root_rank` is calculated before non-anchor spans are discarded, so root paths can skip numbers without changing their relative order.
Tied timestamps remain unstable; adding `span_id` after `start_time` in both windows would make them deterministic.

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

The production projection names every required column, so schema changes show up in the query diff.

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

The result contains the whole trace with each matching span marked, so the waterfall can highlight matches, retain their ancestors, and collapse unrelated branches without rebuilding the tree.

## Orphans

A capture can be missing a parent because a batch was dropped or the data was trimmed.
When that happens, the anchor condition promotes its child to depth zero and the normal walk continues through everything below it.

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
The production query appends those rows to the normal walk, marking recovered spans as `salvaged` and the span whose parent link closes the loop as `cyclePoint` so the UI can explain what happened.

Carrying ancestry makes each recursive row wider, so that work stays in the fallback and runs only when the normal query leaves spans behind.

## Rendering

By the time the response reaches Svelte, DuckDB has fixed the vertical display order and attached a depth to each span.
It also sends each span's start offset and duration, which Svelte converts into horizontal positions and widths.
Interactions still need a local view of the topology, so Svelte derives structural maps for collapsing, search reveal, and keyboard navigation from row order and depth.
It cannot safely trust `parent_span_id`, which may name a missing parent or close a cycle.
