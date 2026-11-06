# ADR 0005: Staging and intermediate models are views, marts are tables

- **Status:** accepted
- **Date:** 2026-11-06
- **Author:** group 7, Step 5 (dbt project)
- **Lesson:** L05

## Context

The dbt project in `transform/` reads the Bronze snapshot through nine
BigQuery external tables and produces three layers: nine staging models, three
intermediate models and five marts (two facts, three dimensions). The choice of
how each layer is stored is one line of configuration in `dbt_project.yml`, and
it decides who pays for what on every read. Measured on 2026-11-06, cache off,
in `europe-west1`:

- The five marts hold 204,280 and 203,431 fact rows and 42,858 products:
  20.1 MiB, 10.9 MiB and 7.2 MiB, with two near-empty dimensions. A full
  `dbt build` of all 86 items takes about 40 seconds.
- The single number "total units sold" (693,320) read from `fct_daily_demand`
  processes **1,627,448 bytes**. The same number recomputed through the staging
  and intermediate views, which re-read the Bronze external tables and redo the
  duplicate-order rule, processes **28,896,703 bytes**: 17.8 times more, for an
  identical answer.
- At the course's on-demand price of $7.50 per TiB (see
  [ADR 0004](0004-warehouse-pricing.md)) the billed difference is about $0.00007
  against $0.0002 per query. **At this size the money is negligible.** The cost
  that matters is the ratio, not the amount: it grows with the data and with
  the number of consumers.
- BigQuery bills at least 10 MiB per table read, and the Step 4 month partitions
  of comparable tables average 0.32 and 0.57 MB.
  ([warehouse.md](../warehouse.md))

## Options considered

| Option | For | Against |
|---|---|---|
| Everything as views | Always fresh. Nothing stored, nothing to rebuild. | Every consumer pays the full scan every time: 17.8 times the bytes here, and the duplicate-order rule is recomputed on every read. |
| Everything as tables | Fast and cheap to read. | Staging is stored a second time for no consumer. A table that is stale between runs hides a bad source change longer, and every layer must be rebuilt in order. |
| **Views, then tables (chosen)** | Fresh, cheap-to-store staging that always reflects its source; fast marts. | A rebuild step, and marts are only as fresh as the last run. |
| Incremental marts | Cheap rebuilds at scale. | We would have to define what "new" means and handle late rows. See the risk below. |

## Criteria

Read cost for the people who query the marts, which is the point of the star;
freshness of the rules that clean the data; storage and rebuild cost at 200,000
rows; and how easily the choice can be reversed. Views win on freshness and
reversibility, tables on read cost. The layers differ on which of these matters.

## Decision

Staging and intermediate models are **views** (`g07_silver`); marts are
**tables** (`g07_gold`). Marts are not partitioned or clustered: at 7 to 20 MiB
a partition would hold well under the 10 MiB minimum BigQuery bills, so pruning
saves nothing, unlike the Step 4 tables that were built for the cost
experiment.

## Consequences

- Easier: a fix in a staging model is live at once for every consumer. Marts are
  cheap to read and to join.
- Harder: gold is only as fresh as the last `dbt build`. Today that is a person
  typing it, until the pipeline is scheduled (Step 7). The source freshness
  checks (`dbt source freshness`) are what tell a reader that the marts may be
  stale.
- The intermediate views, notably the duplicate-order rule, are recomputed when
  the marts and their tests run. That is acceptable at about 40 seconds and a
  few megabytes.

## What would change our mind

- **The marts take about an hour to rebuild.** We would move the two facts to
  **incremental**, partitioned by `order_date`. The cost is real here:
  `int_orders_deduplicated` decides which copy of an order to drop by ranking
  every order of a buyer, payment and basket together, and genuine repeat
  purchases of the same basket are up to 966 days apart. A model that only
  reprocesses new days would rank incomplete groups and could keep the wrong
  copy. We would need to rebuild the whole group of any buyer touched by a new
  order, and decide how a late twin is handled.
- **A consumer needs minute-fresh numbers.** We would put that one mart back to a
  view, or a materialised view, and accept paying the full scan on each read.
  Step 4 already runs a materialised view for daily demand
  ([warehouse-tuning.md](../warehouse-tuning.md)).
- **Orders start arriving continuously (change data capture).** The same
  incremental question appears, with late and updated rows as the normal case.
- **The marts grow past what a partition-free table can read cheaply.** Add
  month partitioning and clustering by `p_id`, as in Step 4.
