with base as (
    select
        organization_id,
        date(created_at) as invoice_date,
        amount_usd as amount_usd,
        payment_amount_usd as payment_usd
    from {{ ref('inmd_invoices') }}
    where status in ('paid', 'unpaid', 'processing')
),

daily_agg as (
    select
        organization_id,
        invoice_date,
        sum(amount_usd) as daily_invoice_usd,
        sum(payment_usd) as daily_payment_usd
    from base
    group by organization_id, invoice_date
),

with_cumulative as (
    select
        *,
        sum(daily_invoice_usd) over (
            partition by organization_id order by invoice_date
        ) as cumulative_invoice_usd,

        sum(daily_payment_usd) over (
            partition by organization_id order by invoice_date
        ) as cumulative_payment_usd
    from daily_agg
),

with_balance as (
    select
        *,
        cumulative_invoice_usd - cumulative_payment_usd as balance_usd,
        lag(balance_usd) over (
            partition by organization_id order by invoice_date
        ) as prev_balance_usd
    from with_cumulative
)

select
    *,
    case
        when prev_balance_usd is null then null
        when prev_balance_usd = 0 then null
        else abs(abs(balance_usd) - abs(prev_balance_usd)) / abs(prev_balance_usd)
    end as balance_change_pct,

    case
        when prev_balance_usd is null or prev_balance_usd = 0 then false
        else balance_change_pct > 0.5
    end as balance_change_gt_50pct,
    {{ dbt_utils.generate_surrogate_key([
        'organization_id'
        , 'invoice_date'
    ]) }} as sk_id
from with_balance
