+++
date = '2025-09-01T08:39:50-07:00'
draft = true
title = "Recursive CTEs for Trace Waterfalls"
description = "DuckDB turns span parent IDs into depth-first rows for otel-desktop-viewer's trace waterfall."
summary = "DuckDB turns span parent IDs into depth-first rows for otel-desktop-viewer's trace waterfall."
tags = ['OpenTelemetry', 'otel', 'otel-desktop-viewer', 'DuckDB', 'SQL', 'CTE', 'traces']
author = 'Mila Ardath'
+++

When I first built [`otel-desktop-viewer`](https://github.com/CtrlSpice/otel-desktop-viewer), incoming traces went into a list in memory and then straight to the frontend.
OTLP represents span parentage as IDs, not child arrays, so turning those spans into a waterfall became the browser's problem.

The frontend indexed spans by ID, filled in missing parents, walked the resulting tree, and flattened it again for rendering.
It worked, but it left the frontend doing a lot of tree bookkeeping.

When I replaced the in-memory list with DuckDB, a Recurse Center colleague happened to demonstrate Conway's Game of Life using recursive CTEs in PostgreSQL.
That gave me a reason to try the trace walk in DuckDB.

I needed DuckDB to return the spans in depth-first order, with a depth on each row.
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

Svelte can render that list without first reconstructing the graph from parent IDs.

## The span table

The real `spans` table also carries status, attributes, resource and scope references, plus the fields displayed in the detail pane.
Only four columns are involved in the walk.

```sql
create table spans (
    trace_id uuid not null,
    span_id uuid not null,
    parent_span_id uuid,
    start_time bigint not null,
    primary key (trace_id, span_id)
);
```

A span ID is only required to be unique within its trace, so the key is `(trace_id, span_id)`.
OpenTelemetry span IDs are eight bytes.
The viewer zero-pads them to sixteen bytes on ingest so DuckDB can store and index them as UUIDs.
The padding is only there for DuckDB storage; recursion does not depend on it.

The simplified table ends up with this query.

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

The joins and window functions are ordinary SQL.
The recursive part starts from roots and orphans, walks through their children, and uses `sort_path` to order the rows.

`search_params` casts the trace ID once.
Bad input casts to `null` and matches no trace, and the caller only has to bind one argument.
My first attempt bound the same trace ID five times, making the query easier to call incorrectly.

## Recursion

Everything above `union all` seeds DuckDB's recursive working table.
In this query, those rows are roots and orphans.

DuckDB runs the query below `union all` against those rows, adds the results to the working table, and repeats with the newly added rows.
The recursion stops when an iteration finds no more children.

A complete trace starts from the span with no parent.

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

## Sort paths

Depth handles indentation.
Waterfall order also depends on where each span sits among its siblings.

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

Materializing `trace_spans` first limits every recursive step to the requested trace.
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

A separate CTE adds search matches after traversal.
Filtering before recursion could drop an unmatched parent and every matching child below it, so the final projection marks matches without removing the surrounding spans.

## Cycles

Missing parents can be handled by treating their children as roots.
Cycles never produce one of those roots.

```text
span-a parent = span-b
span-b parent = span-a
```

Because a span has only one parent, a cyclic component cannot also lead back to a root.
The normal walk never reaches either span above.

The backend compares the number of rows returned with the number in `trace_spans`.
If any are missing, it runs a second query that chooses entry points and carries a list of visited IDs so it can recover cycles without looping.

I tried cycle detection in the main query first.
It moved a representative fetch from 32.5 ms to 35.2 ms across three interleaved rounds, even though none of the 122,224 spans in that capture belonged to a cycle.

Cycle recovery now runs only when the first query leaves spans behind.

## Rendering

Each returned row already has its waterfall position and depth, leaving Svelte to handle collapsing, search highlighting, keyboard navigation, and timeline drawing.
