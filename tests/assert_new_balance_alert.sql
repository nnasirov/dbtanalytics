{{ config(
    enable = true,
    severity = 'warn',
    warn_if = '>1',
    notify = true,
    meta={
        "description": "This test checks if balance changes 50% day over day",
        "owner": "@n.nasirov",
        "channel": "balance_alert"
    }
) }}

-- If this warning is thrown then please contact the devs and the Data Science team

with max_date as (
    select max(invoice_date) as current_date
    from {{ ref('fct_organization_balance') }}
),

filtered as (
    select *
    from {{ ref('fct_organization_balance') }} fb
    join max_date md on true
    where fb.invoice_date >= md.current_date - interval '5 day'
      and fb.balance_change_gt_50pct
)

select *
from filtered
