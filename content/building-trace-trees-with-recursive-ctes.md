+++
date = '2025-09-01T08:39:50-07:00'
draft = true
title = "Building Trace Trees With Recursive CTEs"
subtitle = "How I stopped worrying about my spans' parentage, and learned to let DuckDb do it"
tags = ['OpenTelemetry', 'Otel', 'DuckDb', 'CTE', 'otel-desktop-viewer']
author = 'Mila Ardath'
+++

## How We Got Here
When I first started building [otel-desktop-viewer](https://github.com/CtrlSpice/otel-desktop-viewer), the project was supposed to be a way to learn Go and get familiar with the observability space. It has since grown in scope, but initial architecture was very much that of a learning project: incoming traces got appended to a list in memory, the list got unceremoniously forwarded to the frontend, and thus the relationship between spans became the frontend's problem.

To get the waterfall view, I first built out my trace tree like so:

```typescript

export type TreeItem = {
	status: SpanDataStatus.present;
	spanData: SpanData;
	children: TreeItem[];
};

export type MissingTreeItem = {
	status: SpanDataStatus.missing;
	spanID: string;
	children: TreeItem[];
};

export type RootTreeItem =
  | TreeItem
  | MissingTreeItem;

export function arrayToTree(spans: SpanData[]): RootTreeItem[] {
  let rootItems: RootTreeItem[] = [];
  let lookup: { [spanID: string]: RootTreeItem } = {};
  let missingSpanIDs: Set<string> = new Set();

  for (let spanData of spans) {
    let { spanID, parentSpanID } = spanData;

    // If the span is not in the lookup structure yet, add it
    if (!lookup[spanID]) {
      lookup[spanID] = {
        status: SpanDataStatus.present,
        spanData: spanData,
        children: [],
      };
    }

    let treeItem = lookup[spanID];

    // Note A:
    // If the span has been added to the lookup structure as a 
    // missing/incomplete parent on a previous pass (see Note B), 
    // update it and mark it present, and remove it from the missing set.
    if (treeItem.status === SpanDataStatus.missing) {
      // Re-assign treeItem as a TreeItem type so that after this if statement,
      // the type system knows that treeItem can only be a TreeItem
      treeItem = {
        status: SpanDataStatus.present,
        spanData: spanData,
        children: treeItem.children,
      };
      // overwrite the stored version since now we know it is present
      lookup[spanID] = treeItem
      missingSpanIDs.delete(spanID);
    }

    // If the current span has no parentSpanID, add it to the rootItems
    if (!parentSpanID) {
      rootItems.push(treeItem);
    } else {
      // Note B:
      // If the current span's parentSpanID is not in the lookup structure 
      // yet, add it as a missing/incomplete span, to be updated in a 
      // subsequent loop if found (see note A)
      if (!lookup[parentSpanID]) {
        lookup[parentSpanID] = {
          status: SpanDataStatus.missing,
          spanID: parentSpanID,
          children: [],
        };
        // Add the partial span to the missing set.
        missingSpanIDs.add(parentSpanID);
      }

      // Finally, add the current span to its parent's children array.
      lookup[parentSpanID].children.push(treeItem);
    }
  }

  // To handle incomplete traces:
  // 1. Sort the missing spans by the earliest start time of their children
  // 2. Appended sorted spand to the end of the rootItems array
  // This way we make sure that the root span is added first (if present),
  // and preserve the visual clarity of the waterfall view
  let missingIDsArray = Array.from(missingSpanIDs).sort(
    (a: string, b: string) => {
      let earliestStartTimeA = getEarliestStartTime(lookup[a].children);
      let earliestStartTimeB = getEarliestStartTime(lookup[b].children);

      return earliestStartTimeA - earliestStartTimeB;
    },
  );

  for (let spanID of missingIDsArray) {
    rootItems.push(lookup[spanID]);
  }

  return rootItems;
}

```

Then did a depth-first traverse of that tree and flattened it into an array where sibling spans were sorted by start time, and depth was retained for proper rendering. In case of missing spans, orphaned subtrees were given a phantom parent span.

```typescript

function orderSpans(spanTree: RootTreeItem[]): SpanWithUIData[] {
  let orderedSpans: SpanWithUIData[] = [];

  for (let root of spanTree) {
    let stack = [
      {
        treeItem: root,
        depth: 0,
      },
    ];

    while (stack.length) {
      let node = stack.pop();
      if (!node) {
        break;
      }
      let { treeItem, depth } = node;

      if (treeItem.status === SpanDataStatus.present) {
        orderedSpans.push({
          status: SpanDataStatus.present,
          spanData: treeItem.spanData,
          metadata: { depth: depth, spanID: treeItem.spanData.spanID },
        });
      } else {
        orderedSpans.push({
          status: SpanDataStatus.missing,
          metadata: { depth: depth, spanID: treeItem.spanID },
        });
      }

      treeItem.children
        .sort((a, b) => {
          if (
            a.status === SpanDataStatus.present &&
            b.status === SpanDataStatus.present
          ) {
            return (
              getNsFromString(b.spanData.startTime) -
              getNsFromString(a.spanData.startTime)
            );
          }
          // This doesn't happen- all missing spans are root,
          // and all children by definition have a present status
          return 0;
        })
        .forEach((child: TreeItem) =>
          stack.push({
            treeItem: child,
            depth: depth + 1,
          }),
        );
    }
  }

  return orderedSpans;
}

```

This was, as the kids say, all a bit much. The functionality should have never been relegated to the frontend to begin with, as it didn't scale for large traces. It worked though. In fact, it worked well enough that even as `otel-desktop-viewer` became more popular and I started the slow process of turning my toy project into a real developer tool, those blocks of code were largely left to do their thing in benign neglect.

Moving the "trace tree stuff" to the backend only became a priority when it was time to start working on a new UI. With the additional complexity of metrics and logs to contend with, I wanted to be more intentional in my approach, and keep the frontend code as simple as possible. 

Around the same time, a colleague from my [Recurse Center](https://www.recurse.com/scout/click?t=747f0201d75cb085087a83eace9ebd1f) batch did a demo about solving Conway's Game of Life using recursive CTEs in PostgreSQL, and suddenly I knew exactly where to move said "tree stuff". See, a non-trivial part of `otel-desktop-viewer`s ongoing redevelopment involved replacing the in-memory trace list with [DuckDB](https://duckdb.org/). Sure, I could have re-implemented my tree-building (and subsequent flattening) code in Go, but half the fun of using a database designed for data analysis is to see how much logic I can (reasonably) offload onto it.

## What's a CTE Again?

A CTE (or Common Table Expression) is essentially a named temporary result set that you can reference within a SQL query. Think of it as creating a virtual table that exists only for the duration of that query. 

Recursive CTEs reference themselves to aggregate data or build hierarchical structures. 
This makes them perfect for tree structures like trace spans, where you need to start with root spans, recursively build the complete hierarchy, and return it as a flat array.

### The Query
Here's the ~~eldritch incantation~~ recursive CTE that replaces all of  that TypeScript code:

```sql
WITH RECURSIVE
-- Define the trace parameter
param(traceID) AS (
    VALUES (?)
),

spans_tree AS (
    -- Anchor: root span and orphans
    SELECT 
        s.TraceID,
        s.TraceState,
        s.SpanID,
        s.ParentSpanID,
        s.Name,
        s.Kind,
        s.StartTime,
        s.EndTime,
        s.Attributes,
        s.Events,
        s.Links,
        s.ResourceAttributes,
        s.ResourceDroppedAttributesCount,
        s.ScopeName,
        s.ScopeVersion,
        s.ScopeAttributes,
        s.ScopeDroppedAttributesCount,
        s.DroppedAttributesCount,
        s.DroppedEventsCount,
        s.DroppedLinksCount,
        s.StatusCode,
        s.StatusMessage,
        0 as depth,
        ARRAY[ROW_NUMBER() OVER (ORDER BY 
            CASE 
                WHEN s.ParentSpanID IS NULL OR s.ParentSpanID = '' THEN 0 
                ELSE 1 
            END,
            s.StartTime
        )] as sort_path
    FROM spans s, param p
	WHERE s.TraceID = p.traceID 
	AND s.ParentSpanID NOT IN (
		SELECT SpanID 
		FROM spans 
		WHERE TraceID = p.traceID
	)
    
    UNION ALL
    
    -- Recursive: Find children of the current span, sorted by StartTime
    SELECT 
        s.TraceID,
        s.TraceState,
        s.SpanID,
        s.ParentSpanID,
        s.Name,
        s.Kind,
        s.StartTime,
        s.EndTime,
        s.Attributes,
        s.Events,
        s.Links,
        s.ResourceAttributes,
        s.ResourceDroppedAttributesCount,
        s.ScopeName,
        s.ScopeVersion,
        s.ScopeAttributes,
        s.ScopeDroppedAttributesCount,
        s.DroppedAttributesCount,
        s.DroppedEventsCount,
        s.DroppedLinksCount,
        s.StatusCode,
        s.StatusMessage,
        st.depth + 1,
        st.sort_path || ARRAY[ROW_NUMBER() OVER (
            PARTITION BY st.SpanID 
            ORDER BY s.StartTime
        )] as sort_path
    FROM spans s, param p
    JOIN spans_tree st ON s.ParentSpanID = st.SpanID AND s.TraceID = st.TraceID
    WHERE s.TraceID = p.traceID
)
-- Return all spans in depth-first order
SELECT 
    TraceID, TraceState, SpanID, ParentSpanID, Name, Kind, 
    StartTime, EndTime, Attributes, Events, Links, 
    ResourceAttributes, ResourceDroppedAttributesCount,
    ScopeName, ScopeVersion, ScopeAttributes, ScopeDroppedAttributesCount,
    DroppedAttributesCount, DroppedEventsCount, DroppedLinksCount,
    StatusCode, StatusMessage, depth
FROM spans_tree
ORDER BY sort_path
```
Let me break down what this query is doing:

## The Parameter CTE:
```sql
param(traceID) AS (VALUES (?))
```

Here, we create a temporary table with the trace ID we want to query. It allows us to reference the `traceID` multiple times in the query without repeating the parameter. Instead of having `WHERE s.TraceID = ?` in multiple places, we can join against `param p` and use `p.traceID`.

While it isn't strictly necessary for the logic to follow, it does make the query cleaner and ensures we only need to pass the parameter once:

```go
rows, err := s.db.QueryContext(ctx, SelectTrace, traceID)
```

As opposed to having to repeat it multiple times:

```go
rows, err := s.db.QueryContext(ctx, SelectTrace, traceID, traceID)
```

Note: Having a `param` CTE is comes in handy during the development process too. Building complex queries is best done in incremental logical steps, and my fist attempt at this one had something like 5 `traceID` arguments. That's about 4 more `traceID` arguments than I can be reasonably trusted with, so building parameter management into the query seemed prudent.

## The Anchor Query (Our Base Case):

The first part of the recursive CTE finds all spans that don't have a parent span within the same trace. This handles the root span (which has no `ParentSpanID`) and orphaned spans (spans with `ParentSpanIDs` that don't correspond to an existing `SpanID` within our trace). These spans get assigned `depth = 0` since they're at the top level of our trace tree.

The `sort_path` ensures that these spans are ordered root span first (if it exists), followed by all orphan spans in order of earliest start time. More on sort paths in a bit.

`UNION ALL` combines the results from our anchor query (root and orphans) with the results from the recursive part (children)

## The Recursive Query:

```sql
FROM spans s, param p
JOIN spans_tree st ON s.ParentSpanID = st.SpanID AND s.TraceID = st.TraceID
WHERE s.TraceID = p.traceID
```
This part of the query finds all the children of the spans we've found so far. This part is pretty straightforward: We link each child span (`s`) to its parent (`st`) through `s.ParentSpanID = st.SpanID`, while ensuring we stay within the same trace:

```sql
st.depth + 1
```
Also important to note that we increment the `depth` by 1 for each level we travel down the tree. This will allow us to render our waterfall correctly on the frontend.

The tricky part here is that each set of sibling spans (spans that share the same parent) must be sorted by ascending start time. This is where the `sort_path` I mentioned earlier comes in clutch.

## Sort Paths 

When building tree structures with recursive CTEs, we need to sort the results in a way that satisfies our constraints while preserving hierarchical relationships. Sort paths solve this by creating a sequence (an array of integers, in our case) that represents each node's position in the tree. As we traverse the tree, we add to the sequence.  

When sorted lexicographically, they produce a depth-first traversal with siblings in the correct order:

```
root-span: [1]
├─ child-span: [1, 1]
│  └─ grandchild-span: [1, 1, 1]
└─ child-span-2: [1, 2]

orphan-span: [2]
└─ orphan-child: [2, 1]
```

When sorted, this gives us: `[1]`, `[1, 1]`, `[1, 1, 1]`, `[1, 2]`, `[2]`, `[2, 1]` - exactly the depth-first order we want. 

###  Anchor Sort Path

```sql
-- Anchor: root and orphans
ARRAY[ROW_NUMBER() OVER (ORDER BY 
    CASE WHEN s.ParentSpanID IS NULL OR s.ParentSpanID = '' THEN 0 ELSE 1 END,
    s.StartTime
)] as sort_path
```

Let's take a closer look at the implementation of our anchor sort path:

1. **`CASE` statement: ** Prioritizes root spans (priority 0) over orphaned spans (priority 1)
2. **`ORDER BY`:** Sorts first by priority (roots before orphans), then by start time
3. **`ROW_NUMBER() OVER`:** Assigns sequential numbers starting from 1 to each span in the sorted result set
4. **`ARRAY[]`:** Wraps the  number in an array, creating sort paths like `[1]`, `[2]`, `[3]` for the root-level spans.

These single-number arrays become the basis for the hierarchical sequences we will build during our recursive traverse.

### Recursive Sort Path

```sql
-- Recursive: children of each parent
st.sort_path || ARRAY[ROW_NUMBER() OVER (
    PARTITION BY st.SpanID 
    ORDER BY s.StartTime
)] as sort_path
```

Here, the implementation looks a little different, because we append to the sequence as we traverse our tree:

1. **`PARTITION BY st.SpanID`:** Groups children by their parent's `SpanID`, so each parent's children are numbered separately
2. **`ORDER BY s.StartTime`:** Sorts siblings by their start time within each parent group
3. **`ROW_NUMBER() OVER`:** Assigns sequential numbers starting from 1 to each child within its parent group
4. **`ARRAY[]`:** Wraps the child's position number in an array
5. **`st.sort_path ||` (concatenation):** Appends the child's position array to the parent's existing sort path

So if a span's parent has sort path `[1, 2]` and that span is chronologically the third child of that parent, we append `[3]` to get `[1, 2, 3]`.

## Putting It Together
```sql
-- Return all spans in depth-first order
SELECT 
    TraceID, TraceState, SpanID, ParentSpanID, Name, Kind, 
    StartTime, EndTime, Attributes, Events, Links, 
    ResourceAttributes, ResourceDroppedAttributesCount,
    ScopeName, ScopeVersion, ScopeAttributes, ScopeDroppedAttributesCount,
    DroppedAttributesCount, DroppedEventsCount, DroppedLinksCount,
    StatusCode, StatusMessage, depth
FROM spans_tree
ORDER BY sort_path
```

This final SELECT brings everything together: All our span data, along with each span's depth information, ordered lexicographically by `sort_path`, which gives us exactly the order we need for a waterfall view: root spans first, then their children in chronological order, then grandchildren, and so on.

The recursive CTE built the tree structure, the sort paths ensured proper ordering, and now we have a single result set that's ready for rendering!
