use karthik_db;

create table orders(
order_id int,
order_date varchar(45),
order_customer_id int,
order_status varchar(45)
)
row format delimited fields terminated by ',' stored as textfile;