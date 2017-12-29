select o.order_id, o.order_date, o.order_status, sum(oi.order_item_subtotal) order_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_id, o.order_date, o.order_status
having sum(oi.order_item_subtotal) > 1000
order by o.order_date, order_revenue desc;

select o.order_id, o.order_date, o.order_status, sum(oi.order_item_subtotal) order_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_id, o.order_date, o.order_status
having sum(oi.order_item_subtotal) > 1000
distribute by o.order_date sort by o.order_date, order_revenue desc;