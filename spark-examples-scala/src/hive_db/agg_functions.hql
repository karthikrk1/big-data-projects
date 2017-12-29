select order_status, count(1) as order_count from orders group by order_status;

select o.order_id, sum(oi.order_item_subtotal) order_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_id
having sum(oi.order_item_subtotal) > 1000;


select o.order_date, round(sum(oi.order_item_subtotal), 2) daily_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_date;