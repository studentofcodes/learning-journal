# 001 — [SQL] Extracting & Deduplicating Regex Matches in Spark SQL
**Date:** 2026-10-27 | **Languages:** SQL (Spark SQL / Databricks) | **Stage:** Pre-roadmap (work practice; early look at Stage 14)

## What I studied
- A real work task: pulling structured codes (format `QT-XXX-XXX-XXX-XXX`) out of free-text comment fields, deduplicating them per record, and joining them to another table.
- Worked through it in a Dia chat, then had my full query reviewed for bugs.
- Reference: [Spark SQL built-in functions](https://spark.apache.org/docs/latest/api/sql/index.html)

## Key concepts
- **`LIKE '%QT-%'`** filters rows, but it can't *extract* anything. It only answers "does this row contain the text?"
- **Regular expression (regex):** a pattern language for text. `QT-(?:[A-Za-z0-9]+-){3}[A-Za-z0-9]+` means "QT-, then 3 groups of letters/digits each followed by a hyphen, then one final group."
  - `[A-Za-z0-9]` = one letter or digit · `+` = one or more · `{3}` = exactly 3 times · `(?: … )` = a group that isn't captured separately.
- **`REGEXP_EXTRACT(col, pattern, 0)`** returns only the **first** match, as a string. `0` = the whole match.
- **`REGEXP_EXTRACT_ALL`** returns **every** match, as an array.
- **`EXPLODE(array)`** turns one row holding an array into one row per element.
- **`DISTINCT` vs `COLLECT_SET`:** `DISTINCT` removes duplicate *rows*. `COLLECT_SET` (used with `GROUP BY`) gathers unique values into an array, one row per group.
- **CTE (`WITH name AS (...)`):** a named step in the query, so I can build the logic in stages and avoid repeating the regex.
- **Window function `ROW_NUMBER() OVER (PARTITION BY x ORDER BY y DESC)`:** numbers rows 1, 2, 3… within each group without collapsing them. Filtering `row_num = 1` keeps the latest row per group.

## How it works under the hood
SQL isn't run in the order it's written. The engine processes it in this **logical order**:
1. `FROM` / `JOIN`: gather the rows
2. `WHERE`: throw out rows that don't pass
3. `GROUP BY`: put rows into groups
4. `SELECT`: compute the columns (regex functions run here, once per row; `EXPLODE` creates the extra rows; window functions like `ROW_NUMBER()` are calculated here)
5. `DISTINCT`: remove duplicate rows
6. `ORDER BY`, then `LIMIT`

Why it matters:
- **I can't filter on `row_num` in the same query that creates it.** `WHERE` runs before `SELECT`, so `row_num` doesn't exist yet. That's why it goes in a CTE first, and gets filtered in the next step.
- **`ORDER BY` inside a CTE does nothing useful.** The engine is free to reorder rows between steps. Only the final `ORDER BY` (or the `ORDER BY` inside `OVER(...)`) is guaranteed.
- In Spark, `DISTINCT`, `GROUP BY`, and `PARTITION BY` all trigger a **shuffle**: rows get moved between machines so matching keys end up together. That's the expensive part of the query, so doing it once (in a CTE) helps.

## Example code
```sql
WITH exploded AS (                         -- step 1: one row per code found
  SELECT
    record_id,                             -- keep the ID so we know where each code came from
    EXPLODE(                               -- turn the array into separate rows
      REGEXP_EXTRACT_ALL(                  -- find EVERY match, not just the first
        comment,
        'QT-(?:[A-Za-z0-9]+-){3}[A-Za-z0-9]+',
        0)                                 -- 0 = return the whole match
    ) AS code
  FROM comments
  WHERE comment LIKE '%QT-%'               -- cheap pre-filter before the costly regex
),
unique_codes AS (                          -- step 2: remove repeats across comments
  SELECT DISTINCT record_id, code
  FROM exploded
)
SELECT u.record_id, u.code, q.status      -- step 3: attach details from another table
FROM unique_codes u
LEFT JOIN quotes q                         -- LEFT = keep codes even with no match
  ON u.code = q.code;
```

## Bugs & fixes
**1. Wrong window function syntax (my bug)**
- I wrote: `row(over(partition by quote_id order by row_updated_timestamp desc))`
- Why it's wrong: `ROW` isn't a window function, and `OVER` isn't a function that wraps things. It's a clause that attaches *after* a function call.
- Fix: `ROW_NUMBER() OVER (PARTITION BY quote_id ORDER BY row_updated_timestamp DESC) AS row_num`
- How to read the error: Spark fails at the parse/analysis step and points to the position near `over`. Start reading at that position. The mistake is usually just before it.

**2. A filter that silently dropped rows (my bug, and the dangerous kind)**
- I wrote: `WHERE rn = 1 AND quote_rn = 1` after LEFT JOINs.
- What happened: records with no code got `quote_rn = NULL`. In SQL, `NULL = 1` isn't false, it's **UNKNOWN**, and `WHERE` keeps only rows that are TRUE. Those records disappeared, **with no error message.**
- Fix: `WHERE rn = 1 AND (quote_rn = 1 OR quote_rn IS NULL)`, or move the `quote_rn = 1` condition into the `JOIN ... ON` clause.
- Lesson: the worst bugs don't crash. Always compare row counts before and after a filter or join.

**3. Only getting one code per comment**
- `REGEXP_EXTRACT` stops at the first match. Fix: `REGEXP_EXTRACT_ALL` + `EXPLODE`.

**4. `LIMIT` without `ORDER BY`**
- Returns *some* rows, not the first or latest. That's fine for a quick look, but don't trust it as a sample.

## Debug drill
This query should list **every** ticket, plus its accepted quote if it has one. Instead, tickets without an accepted quote vanish. Why?
```sql
SELECT t.ticket_id, q.code, q.status
FROM tickets t
LEFT JOIN quotes q
  ON t.ticket_id = q.ticket_id
WHERE q.status = 'ACCEPTED';
```
<details><summary>Solution</summary>

For tickets with no matching quote, `q.status` is NULL. `NULL = 'ACCEPTED'` is UNKNOWN, so `WHERE` drops the row. A WHERE condition on the right-hand table quietly turns a LEFT JOIN into an INNER JOIN. Fix: move the condition into the join:
```sql
LEFT JOIN quotes q
  ON t.ticket_id = q.ticket_id
 AND q.status = 'ACCEPTED'
```
This is the same family of bug as #2 above.
</details>

## Self-check
1. Why can't I write `WHERE row_num = 1` in the same SELECT that defines `row_num`?
2. A comment contains three codes, two of them identical. How many rows come out of `EXPLODE(REGEXP_EXTRACT_ALL(...))`, and how many after `DISTINCT`?
3. When would I pick `COLLECT_SET` + `GROUP BY` over `SELECT DISTINCT`?
4. What does `NULL = NULL` evaluate to, and how do I check for NULL correctly?
5. My query runs without errors but returns 20% fewer rows than expected. What are the first two things I check?

## Next up
002 — [Python] Setup & how Python runs code (Roadmap stage 1). SQL picks back up at stage 14, where this entry becomes a head start for `[Python × SQL]`.
