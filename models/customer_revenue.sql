select customer_id, count(*) as order_count, sum(amount) as revenue
from {{ ref('stg_orders') }}
group by customer_id
