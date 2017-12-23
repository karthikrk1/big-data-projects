# This is to create a hive table as a join and do some computations
# Usage hive -f sqoop_export_preparation_2.hql

# This HQL just creates a join of the two tables and finds the daily revenue
# for the month of JULY 2017.

create table daily_revenue as
select order_date, sum(order_item_subtotal) as daily_revenue
from orders join order_items on
order_id = order_item_order_id
where order_date like '2017-07%'
group by order_date