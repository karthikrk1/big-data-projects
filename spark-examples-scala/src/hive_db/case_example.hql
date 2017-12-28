// case
select order_status, case when order_status IN ('CLOSED', 'COMPLETE') then 'No Action'
                          when order_status IN ('ON_HOLD', 'PROCESSING', 'PENDING', 'PAYMENT_REVIEW', 'PENDING_PAYMENT') then 'Pending Action'
                          else 'Risky'
                          end from orders limit 100;


// nvl

select nvl(order_status, 'missing') from orders limit 100;