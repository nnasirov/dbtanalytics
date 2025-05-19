with invoices as (
    select
        organization_id,
        date(created_at) as invoice_date,
        amount_usd as amount_usd,
        payment_amount_usd as payment_usd
    from {{ ref('inmd_invoices') }}
    where status in ('paid', 'unpaid', 'processing')
),

aggregated as (
    select
        organization_id,
        count(*) as total_invoices,
        sum(amount_usd) as total_invoice_usd,
        sum(payment_usd) as total_payment_usd,
        min(invoice_date) as first_invoice_date,
        max(invoice_date) as last_invoice_date
    from invoices
    group by organization_id
)

select
    o.organization_id,
    o.first_payment_date::date as first_payment_date,
    o.last_payment_date::date as last_payment_date,
    o.legal_entity_country_code,
    o.count_total_contracts_active,
    o.created_date::timestamp as organization_created_at,
    datediff(day, o.created_date::timestamp, current_date) as organization_age_days,
    a.total_invoices,
    a.total_invoice_usd,
    a.total_payment_usd,
    a.first_invoice_date,
    a.last_invoice_date,
    round(a.total_invoice_usd,0) > round(a.total_payment_usd,0) as has_balance_due,
    round(a.total_payment_usd / nullif(a.total_invoice_usd, 0),2) as invoice_payment_ratio
from {{ ref('organizations') }} o
left join aggregated a
  on o.organization_id = a.organization_id
