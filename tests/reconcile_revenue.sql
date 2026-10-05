select 1 as mismatch
where (select sum(amount) from {{ ref('stg_orders') }})
   <> (select sum(revenue) from {{ ref('customer_revenue') }})
