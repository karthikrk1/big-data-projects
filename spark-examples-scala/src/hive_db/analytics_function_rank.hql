select * from (
select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id),2) order_revenue,
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) pct_revenue,
round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue,
rank() over (partition by o.order_id order by oi.order_item_subtotal desc) rnk_revenue,
dense_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) dense_rnk_revenue,
row_number() over (partition by o.order_id order by oi.order_item_subtotal desc) rn_order_by_revenue,
row_number() over (partition by o.order_id ) rn_revenue,
percent_rank() over (partition by o.order_id ) pct_rnk_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')) q
where q.order_revenue >= 1000
order by q.order_date, order_revenue desc, rnk_revenue;