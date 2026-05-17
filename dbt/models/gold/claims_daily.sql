-- Gold: daily claim volume + dollar by plan type.
-- Feeds the ops tab on the member-360 dashboard.

select
    service_date                              as dos_date,
    'commercial'                              as plan_type,    -- TODO: join Synthea payer plan
    count(*)                                  as claims_count,
    sum(total_charge)                         as paid_amount,
    avg(total_charge)                         as avg_paid_per_claim
from {{ ref('claims_enriched') }}
group by service_date, plan_type
order by service_date desc
