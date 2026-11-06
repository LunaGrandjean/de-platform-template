# ADR 0004: Keep on-demand compute with query byte caps

- Status: accepted for the current workload; no capacity reservation purchased
- Date: 2026-11-06
- Authors: group 7 (warehouse by Emile Gadenne; review fixes by Luna Grandjean)
- Related: [table layout](0004-warehouse-tables.md), [tuning](../warehouse-tuning.md)

## Workload and evidence

The existing three native business queries process 277,004, 1,625,204 and
109,808 bytes, respectively, with cache disabled (2026-11-05 job evidence).
Their mean is **670,672 bytes/query**. This is a measured size, not an observed
production query rate. For planning, assume **20 queries/day**, 30 days/month,
and a peak of **three concurrent interactive queries** during a group review.
There is no continuous dashboard or measured concurrency SLA yet. Record real
usage and slot time before purchasing capacity.

The course's Belgium (`europe-west1`) on-demand rate is **$7.50/TiB**.
Google's public pricing page was checked on 2026-11-06: Standard edition,
Belgium, pay as you go is **$0.044/slot-hour**. The page initially displays
Iowa ($0.04); use the Belgium selection/embedded regional table, not that
default. [Price evidence](../evidence/step4/pricing-reference.json) records the
lookup and the public page hash. Storage/ingestion are separate from compute.

## Numerical comparison

For 600 monthly queries:

| Scenario | Calculation | Monthly compute estimate |
|---|---|---:|
| Course estimate from processed bytes | `600 * 670672 / 2^40 * 7.50` | $0.002745 before allowance |
| Billing-minimum illustration, one native table/query | `600 * 10485760 / 2^40 * 7.50` | $0.042915 before allowance |
| 100× data per query, same query count | `600 * 670672 * 100 / 2^40 * 7.50` | $0.274488 before allowance |
| Illustrative 50 slots allocated 24 hours/day | `50 * 24 * 30 * 0.044` | $1,584 |
| Illustrative 50 slots used 2 hours/day | `50 * 2 * 30 * 0.044` | $132 |

The first **1 TiB/month** of on-demand usage is free at the applicable billing
account. This school project is shared: we have not audited how much allowance
other users consume. If the allowance is fully available, these on-demand
scenarios cost $0; use the before-allowance figures when it has been consumed.
The 100× scenario is a linear scan estimate, not a measured benchmark.

Capacity examples model provisioned/billed slot-hours, not a guaranteed slot
need or the cheapest possible autoscaling setup. Baseline slots incur cost
when idle; autoscaling and fluid-scaling options have different billing
behaviour. No commitment discount, reservation, assignment or sustained
utilisation is assumed to exist. A two-hour/day model is illustrative; our
short bursts could have fewer billed hours and must be measured.

With the full 1 TiB allowance available, the continuous scenario crosses over at
`(1584 / 7.50) + 1 = 212.2 TiB/month` of billed query volume before allowance.
Without available allowance it is `211.2 TiB/month`. At **1 GiB/query**, the
full-allowance threshold is **7,244 queries/day** (rounded up), for 30 days.
For the two-hour scenario it is `(132 / 7.50) + 1 = 18.6 TiB/month`, or
**635 queries/day** at 1 GiB/query. This hypothetical larger query exceeds our
current decimal 1 GB cap, so it would first require an explicit budget review.

General test: compare `max(monthly_billed_TiB - available_free_TiB, 0) * 7.50`
with `measured_billed_slot_hours * regional_slot_hour_price`. Processed bytes
are used for the course's tuning/cost examples; billing minima and available
allowance must be applied to the invoice model.

## Decision and consequences

Keep **on-demand**, native partition/column pruning and a default **1 GB cap per
query** in the warehouse SDK. Keep business duplicates and Bronze unchanged.
On-demand has bill exposure without these controls, and the per-query cap is
not a project-wide monthly budget. Queries run outside this script need the
same console/SDK setting. Alert and review total usage with the project owner.

Reopen this ADR when attributable group usage exceeds **169.76 TiB/month**
(80% of the full-allowance continuous crossover), concurrency regularly exceeds
the assumed three jobs, or interactive queueing becomes unacceptable. For a
bursty reservation model, reopen sooner at **14.88 TiB/month** (80% of 18.6)
and collect a week of slot utilisation. Reconsideration is not automatic
purchase: first simulate actual slot-hours, peak throughput, edition features
and the shared project's reservations, then obtain a reviewed budget decision.

On-demand wins simplicity and low cost here; capacity can improve workload
isolation and predictable throughput for a sustained workload, but buying it
now would mostly pay for unused capacity.

## References

- [Official BigQuery regional prices](https://cloud.google.com/bigquery/pricing)
- [Slots and autoscaling](https://cloud.google.com/bigquery/docs/slots)
- [Cost estimation and safeguards](https://cloud.google.com/bigquery/docs/best-practices-costs)
- [Recorded queries](../evidence/step4/costed-queries.json)
