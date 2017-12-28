use karthik_orc;

create table order_items(
  order_item_id int,
  order_item_order_id int,
  order_item_product_id int,
  order_item_quantity int,
  order_item_subtotal float,
  order_item_product_price float
)
stored as orc;