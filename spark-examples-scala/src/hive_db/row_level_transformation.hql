// select only year and month from order_date and cast to int

select cast(date_format(order_date, 'YYYYMM') as int) from orders limit 5;