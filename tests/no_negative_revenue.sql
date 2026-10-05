select * from {{ ref('customer_revenue') }} where revenue < 0
