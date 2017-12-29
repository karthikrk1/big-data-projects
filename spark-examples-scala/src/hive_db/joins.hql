select o.*, c.* from orders o, customers c where o.order_customer_id = c.customer_id limit 10;

select o.*, c.* from orders o join customers c on o.order_customer_id = c.customer_id limit 10;

select o.*, c.* from orders o inner join customers c on o.order_customer_id = c.customer_id limit 10;

select o.*, c.* from customers c left outer join orders o on c.customer_id = o.order_customer_id limit 10;

select count(1) from orders o inner join customers c on o.order_customer_id = c.customer_id limit 10;

select count(1) from customers c left outer join orders o on c.customer_id = o.order_customer_id limit 10;

select c.* from customers c left outer join orders o on c.customer_id = o.order_customer_id where o.order_customer_id is null limit 10;