+++
date = '2025-09-01T08:39:50-07:00'
draft = true
title = "Generating Trace Waterfalls with Recursive CTEs in DuckDB"
description = "A recursive CTE moves trace-tree construction into DuckDB, in keeping with my long-term strategy of making the database do all the work."
summary = "Using OpenTelemetry traces as a concrete case study, here's what a recursive CTE looks like when it has to survive contact with production data."
tags = ['OpenTelemetry', 'otel', 'otel-desktop-viewer', 'observability', 'distributed tracing', 'traces', 'trace waterfall', 'DuckDB', 'SQL', 'CTE']
author = 'Mila Ardath'
+++

In [`otel-desktop-viewer`](https://github.com/CtrlSpice/otel-desktop-viewer), I shape DuckDB query results around how the interface will use them.
That lets me make storage and computation tradeoffs around how people navigate the result, rather than preserving a generic intermediate representation.
For the waterfall, the query returns spans in depth-first order, along with each span's depth and whether it matched the current search.
Keeping the topology in DuckDB avoids another implementation in Go or the browser and keeps filtering, ordering, and projection together.
If I've made those tradeoffs well, the interface stays snappy, or at least snap-adjacent, even when the database is full of large traces.
This post walks through the recursive CTE that builds the response and handles missing parents, sibling order, and cycles.

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

The real `spans` table also carries status fields, an array of attribute IDs, resource and scope IDs, and the other fields shown in the detail pane.
The walk starts from four stored columns.

```sql
create table spans (
    trace_id uuid,
    span_id ubigint not null,
    parent_span_id ubigint,
    start_time bigint,
    primary key (trace_id, span_id)
);
```

A span ID is only required to be unique within its trace, so the key is `(trace_id, span_id)`.
There is deliberately no foreign key on `parent_span_id`: spans can arrive before their parents, and a partial capture may never contain the parent at all.

OpenTelemetry span IDs are eight bytes, which fit exactly in a DuckDB `ubigint`.
On ingest, an empty parent span ID becomes SQL `null`; nonempty parent IDs use the same type as `span_id`.

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

`search_params` casts the trace ID once.
Bad input casts to `null` and matches no trace, and the caller only has to bind one argument.
A one-row parameter CTE is useful whenever the same input appears in several parts of a query: bind it once, then refer to the named column.

## Recursion

The first `select` inside `spans_tree`, above `union all`, is the anchor member.
It seeds the result with roots and orphans.

On each iteration, the recursive member below `union all` reads the rows produced by the previous iteration and finds their children.
DuckDB adds those rows to the accumulated result, then exposes them as the next iteration's input.
The recursion stops when an iteration finds no more children.

A complete trace normally starts from a span with no parent.

```sql
where r.parent_span_id is null
```

A capture can be missing a parent because a batch was dropped or the data was trimmed.
I still want the child and everything below it in the waterfall.

```sql
or r.parent_span_id not in (select span_id from trace_spans)
```

`not in` is safe here because the primary key prevents `span_id` from being `null`.
The orphan becomes another depth-zero entry.

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

`ranked` already contains only the requested trace, so `span_id` is unambiguous inside the walk.
Joins back to unrestricted tables use both `trace_id` and `span_id`.

`union all` does not create duplicate placements on the normal path because each stored span has one parent, giving every acyclic span one route from an anchor.
A cyclic component has no anchor, so this walk omits it rather than looping; the fallback path handles it later.

## Sort paths

Depth handles indentation, but recursive discovery order is not display order.
The waterfall also needs each span's position among its siblings.

`ranked` numbers the siblings once.

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

DuckDB's list ordering does what the waterfall needs.
A prefix comes before the longer paths below it, and sibling ranks keep neighbouring subtrees in start-time order.

`root_rank` includes non-anchor spans, so the numbers can skip.
Sorting still works because their relative order is unchanged.

Tied timestamps currently have no stable order.
Adding `span_id` after `start_time` in both windows would make them deterministic.

## Query performance

The first version repeatedly scanned more data than it needed.
The recursive arm joined the full `spans` table once per tree level, making trace fetches slower as unrelated traces piled up.

`trace_spans` filters the input to the requested trace.
Forcing it to materialize prevents DuckDB from inlining that filter back into each reference during recursion.
On a store with 2.3 million spans, fetching a 159-span trace fourteen levels deep went from 54 ms to 5 ms.

The first version also calculated `row_number()` inside the recursive arm.
DuckDB reran the window operator at every depth.
On a 122,000-span store, that operator accounted for 27.9 ms of a 46 ms trace fetch.

Sibling order does not change between recursive steps, so `ranked` calculates it once.
I compared row counts and depths, then hashed the span IDs in `sort_path` order.
Both versions returned the same result.

The timings describe the captures I tested.
The profiles tied the first slowdown to total store size and the second to repeated window work at every depth.

## After the walk

The recursive rows only carry IDs, timing, depth, and `sort_path`.
Payload fields join after recursion, keeping each recursive row narrow.

The production query adds those columns after recursion finishes.

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

The real projection names every required column.
Schema changes then show up in the query diff.

A separate CTE identifies search matches after traversal.
Filtering the recursive input would change the tree, removing ancestor context and potentially making matching descendants unreachable, so the final projection marks matches without removing the surrounding spans.

## Cycles

Missing parents can be handled by treating their children as roots.
Cycles never produce one of those roots.

```text
span-a parent = span-b
span-b parent = span-a
```

Because a span has only one parent, a cyclic component cannot also lead back to a root.
The normal walk never reaches either span above.

The normal query returns `count(trace_spans) - count(tree)` as a separate integer beside the trace JSON.
If that count is nonzero, the backend reruns the whole trace with a salvage query instead of merging a recovered fragment into the first result.
The salvage query seeds a walk from every unreached span, carries a list of visited IDs to stop at cycles, and keeps the earliest placement for each span.
Recovered spans are marked `salvaged`, and the span whose parent link closes the loop is marked `cyclePoint`, so the UI can show what happened.

I tried cycle detection in the main query first.
It moved a representative fetch from 32.5 ms to 35.2 ms across three interleaved rounds, even though none of the 122,224 spans in that capture belonged to a cycle.

Cycle recovery now runs only when the first query leaves spans behind.

## Rendering

The response arrives in vertical display order with a depth attached to each span.
DuckDB also sends each span's start offset and duration; Svelte converts those values into horizontal positions and widths.
For collapsing, search reveal, and keyboard navigation, Svelte derives structural maps from row order and depth rather than trusting `parent_span_id`, which may name a missing parent or close a cycle.
