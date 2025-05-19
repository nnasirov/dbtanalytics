
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

select  
        invoice_id
      , transaction_id
      , organization_id
      , status
      , payment_method
      , amount * fx_rate as amount_usd
      , payment_amount * fx_rate_payment as payment_amount_usd
      , created_at
from {{ ref('invoices')}}
