create table customers (
  customer_id int,
  customer_fname varchar(45),
  customer_lname varchar(45),
  customer_email varchar(45),
  customer_password varchar(45),
  customer_street varchar(255),
  customer_city varchar(45),
  customer_state varchar(45),
  customer_zipcode varchar(45)
) row format delimited fields terminated by ',' stored as textfile;