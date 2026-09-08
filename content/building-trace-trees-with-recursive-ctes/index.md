+++
date = '2025-09-01T08:39:50-07:00'
draft = true
title = "Generating Trace Waterfalls with Recursive CTEs in DuckDB"
description = "DuckDB returns the trace waterfall ordered, annotated, and ready to render."
summary = "A recursive SQL walk with orphan promotion, depth-first sort paths, search annotations, and cycle recovery."
tags = ['OpenTelemetry', 'otel', 'otel-desktop-viewer', 'observability', 'distributed tracing', 'traces', 'trace waterfall', 'DuckDB', 'SQL', 'CTE']
author = 'Mila Ardath'
+++

A trace looks like a tree in the waterfall, but it does not arrive as one.
The database stores one row per span, with each child pointing to its parent.
Turning those rows into a display order sounds like a sort until a parent's next sibling starts before one of its descendants.
In [`otel-desktop-viewer`](https://github.com/CtrlSpice/otel-desktop-viewer), I use a recursive CTE in DuckDB to build that order before the spans reach the browser.

## Start with rows

Let's use one small trace all the way through the query.
The offsets are measured from the trace's earliest span:

| name | `span_id` | `parent_span_id` | start offset |
| --- | ---: | ---: | ---: |
| root | 1 | `null` | 0 ms |
| authenticate | 2 | 1 | 100 ms |
| checkout | 3 | 1 | 120 ms |
| fetch-user | 4 | 2 | 150 ms |

`authenticate` and `checkout` are siblings, so their start times put `authenticate` first.
Its descendant, `fetch-user`, belongs with that subtree even though `checkout` started earlier.
The display order must therefore be:

```text
Name                Start offset
--------------------------------
root                         0 ms
├── authenticate           100 ms
│   └── fetch-user         150 ms
└── checkout               120 ms
```

The production `spans` table is much wider, but the walk needs only four columns:

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

## Prepare the walk

The caller supplies something we hope is a `trace_id` as `?`.
`search_params` casts it once, and `trace_spans` filters the store to that trace while leaving the payload columns behind.

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

Using `try_cast` means bad input becomes `null` and cleanly matches no trace.
`materialized` guarantees one evaluation and gives every later reference the same trace-sized relation.
The explicit boundary keeps that property independent of DuckDB's inlining heuristics.

Omitting the repeated `trace_id` and displaying each absolute `start_time` as an offset from the trace start, our materialized rows now look like this:

| `span_id` | `parent_span_id` | `start_time` |
| ---: | ---: | ---: |
| 1 | `null` | 0 ms |
| 2 | 1 | 100 ms |
| 3 | 1 | 120 ms |
| 4 | 2 | 150 ms |

SQL relations have no implicit order; the table is shown by start time only to keep the example easy to follow.

## Rank the rows

Before walking anything, the query assigns two positions.
`sibling_rank` records where a span sits among rows with the same parent.
`root_rank` gives every possible depth-zero starting row one global position.
This trace has one true root; the production query can admit other starting rows when a capture is incomplete, which we will return to later.

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

Materializing `ranked` runs both windows once before the walk and lets every recursive level reuse their results.
For the running trace, those results are:

From here on, `name` appears in intermediate tables only as a reader label; the recursive relations still carry IDs, timing, ranks, depths, and paths.

| name | `span_id` | `sibling_rank` | `root_rank` |
| --- | ---: | ---: | ---: |
| root | 1 | 1 | 1 |
| authenticate | 2 | 1 | 2 |
| checkout | 3 | 2 | 3 |
| fetch-user | 4 | 1 | 4 |

Only anchors use `root_rank`, so the other values in that column will not become path roots.
The recursive member uses `sibling_rank` whenever it adds a child.

## Walk the tree

The complete statement begins with `with recursive`.
Within it, `spans_tree` has two parts: the anchor member seeds depth-zero rows, and the recursive member repeatedly adds their children.
The production anchor also accepts a span whose reported parent is missing; that second condition does not affect this trace, and we will return to it after the healthy path.

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

The anchor places `root` at depth zero with a one-item path.
On each iteration, DuckDB reads the rows produced by the previous iteration, finds their children, and appends those rows to the result.
The walk stops when an iteration finds no more children.

Because `ranked` contains only the requested trace, the recursive self-join can match on `span_id` alone.
Later joins back to unrestricted tables use both `trace_id` and `span_id` to preserve that scope.

Ignoring output order for the moment, `spans_tree` has attached a depth and path to every row:

| name | `depth` | `sort_path` |
| --- | ---: | --- |
| root | 0 | `[1]` |
| authenticate | 1 | `[1, 1]` |
| checkout | 1 | `[1, 2]` |
| fetch-user | 2 | `[1, 1, 1]` |

## Sort paths

The query needs one value whose ordinary sort order produces depth-first traversal.
`sort_path` is the route to a span through the sibling positions above it: the root starts at `[1]`, its first child appends `1`, and that child's first child appends another `1`.
`checkout` is the root's second child, so its path is `[1, 2]` even though it started before `fetch-user`.

```text
root                 [1]
├── authenticate     [1, 1]
│   └── fetch-user   [1, 1, 1]
└── checkout         [1, 2]
```

DuckDB compares lists lexicographically.
Each prefix sorts before the longer paths below it, while sibling ranks keep neighbouring subtrees in start-time order.
That puts `[1, 1, 1]` before `[1, 2]`, producing the display order we wanted.

```sql
select trace_id, span_id, parent_span_id, start_time, depth
from spans_tree
order by sort_path;
```

Tied timestamps remain unstable; adding `span_id` after `start_time` in both windows would make them deterministic.
The [complete production query](https://github.com/CtrlSpice/otel-desktop-viewer/blob/main/desktopexporter/internal/store/queries/spans/search_spans.sql) carries this structure into the payload and JSON stages below.

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
Removing non-matches would discard ancestor context and change the depths and paths the query just built.

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
  { "name": "root", "depth": 0, "matched": false },
  { "name": "authenticate", "depth": 1, "matched": false },
  { "name": "fetch-user", "depth": 2, "matched": true },
  { "name": "checkout", "depth": 1, "matched": false }
]
```

## Render the healthy trace

By now DuckDB has fixed the vertical order and attached a depth.
The final JSON macro turns absolute timestamps into the position and width the waterfall needs:

```sql
'start', ts.start_time - trace_start_ns,
'dur', ts.end_time - ts.start_time
```

The front end turns those values into indented rows and horizontal bars.
Its virtual list mounts only the visible rows, while the same order and depth produce the maps used for collapsing, search reveal, and keyboard navigation.
The browser never has to decide the tree's order or depth again; it only renders and interacts with that ordered list, which is how it stays snappy (or at least snap-adjacent).

{{< figure src="/building-trace-trees-with-recursive-ctes/healthy-search-context.png" alt="The healthy root trace rendered as a waterfall. Authenticate and checkout are children of root, fetch-user is nested beneath authenticate and highlighted as the direct search match, and each row has a horizontal duration bar." caption="The healthy subtree preserves depth-first order while marking fetch-user as the direct search match." >}}

## When traces misbehave

A complete trace forms a tree, but a development tool also receives partial and malformed telemetry.
A dropped batch can remove a parent, and a bad parent link can create a cycle.
Neither should make spans disappear or leave the database walking forever.

The screenshots below use the same healthy relationships, plus these rows:

| name | reported parent | condition |
| --- | --- | --- |
| orphan-root | missing span | parent absent |
| orphan-child | orphan-root | child of the promoted orphan |
| early-off-cycle-child | cycle-a | descendant of the cyclic component |
| cycle-a | cycle-b | closes the cycle |
| cycle-b | cycle-a | closes the cycle |

### Orphans

The anchor condition we deferred earlier treats a span whose reported parent is absent as another depth-zero starting row.
Promotion changes only its place in the display tree; the stored `parent_span_id` remains faithful to the telemetry.
The normal walk then continues through its descendants.

`root_rank` was calculated before the anchor filter, so non-anchor spans still consumed numbers.
An orphan may therefore begin at `[5]` rather than the next visible root number:

```text
orphan-root          [5]
└── orphan-child     [5, 1]
```

The gap does not change its order relative to the healthy root.
The primary key prevents `span_id` from being `null`, so the anchor's `not in` check is safe here.

### Cycles

A cycle has no depth-zero entry point.
If `cycle-a` reports `cycle-b` as its parent and `cycle-b` reports `cycle-a`, the normal walk reaches neither.

The normal query reports that gap as `count(trace_spans) - count(tree)`, returned as a separate integer beside the trace JSON.
When the count is nonzero, the backend reruns the whole trace with a salvage query rather than merging a fragment into the first response.

Every unreached span gets an `entry_rank` from its `start_time, span_id` order, then seeds a candidate walk.
Each candidate tracks the IDs it has visited and stops before repeating one.
Because a span can appear in several candidates, deduplication keeps the placement with the lowest `entry_rank`, then the shallowest depth.
A retained root carries `cyclePoint` when its reported parent appears in the same candidate chain; in a multi-span cycle, that parent appears below it.

`early-off-cycle-child` starts first, so it receives `entry_rank` 1 and seeds a one-row candidate.
It also appears below `cycle-a` in later candidates, but deduplication keeps that earlier depth-zero placement.
That leaves `cycle-a` and `cycle-b` with entry ranks 2 and 3.
The diagram focuses on the competing placements of those two cycle members and omits the already-settled child branch:

{{< mermaid >}}
flowchart TB
    accTitle: Recovering a two-span cycle
    accDescr: After an earlier off-cycle child receives entry rank one, candidate walks begin at cycle-a with rank two and cycle-b with rank three. Each walk stops when it encounters an already visited span, then both placements of each cycle member are compared and those from the cycle-a candidate are kept.

    subgraph first["Candidate from cycle-a"]
        A1["cycle-a<br/>entry_rank 2, depth 0<br/>visited: [a]"] --> B1["cycle-b<br/>entry_rank 2, depth 1<br/>visited: [a, b]"]
        B1 -.-> S1["cycle-a already visited<br/>stop"]
    end

    subgraph second["Candidate from cycle-b"]
        B2["cycle-b<br/>entry_rank 3, depth 0<br/>visited: [b]"] --> A2["cycle-a<br/>entry_rank 3, depth 1<br/>visited: [b, a]"]
        A2 -.-> S2["cycle-b already visited<br/>stop"]
    end

    A1 --> KA{"cycle-a<br/>keep A1"}
    A2 --> KA
    B1 --> KB{"cycle-b<br/>keep B1"}
    B2 --> KB
    KA --> F["Recovered display chain<br/>cycle-a: depth 0, cyclePoint<br/>cycle-b: depth 1"]
    KB --> F
    F --> R["Append to the normal rows<br/>and apply final ordering"]

    class A1,B1 kept
    class A2,B2 discarded
{{< /mermaid >}}

Between the two cycle entries, `cycle-a` ranks first, so A1 wins `cycle-a` and B1 wins `cycle-b`.
`cycle-a` carries `cyclePoint` because its reported parent appears below it in that chain.
`early-off-cycle-child` keeps its lower-ranked placement as a separate depth-zero row.
Its reported parent belongs to the later recovered cycle rather than its own candidate chain, so that row remains unmarked.

The complete recovery query is in [`salvage_spans.sql`](https://github.com/CtrlSpice/otel-desktop-viewer/blob/e62210dc8bc4e0f9465e672e21a292a0c4fc5f36/desktopexporter/internal/store/queries/spans/salvage_spans.sql#L75-L150).
Carrying ancestry and a complete relative path makes each recursive row wider, so that work stays in the fallback and runs only when the normal query leaves spans behind.

{{< figure src="/building-trace-trees-with-recursive-ctes/search-context.png" alt="A trace waterfall filtered to fetch-user. The matching fetch-user row is highlighted beneath root and authenticate, while the descendants of unrelated orphan and cycle branches are collapsed." caption="A search for fetch-user keeps its path open and folds unrelated subtrees." >}}

{{< figure src="/building-trace-trees-with-recursive-ctes/cycle-recovery.png" alt="A synthetic trace waterfall with a normal root tree, an orphan promoted to depth zero, an early off-cycle child, and a recovered two-span cycle. The selected cycle-a row has a biohazard marker, and its detail panel explains that its parent points into its own subtree." caption="An orphan, an early off-cycle child, and a recovered two-span cycle in one synthetic trace." >}}

Healthy or recovered, the response keeps one contract.
DuckDB returns ordered rows with depth, timing, search, and recovery annotations while `parent_span_id` preserves what the instrumentation reported.
The front end renders and interacts with that display topology rather than inventing another one.
