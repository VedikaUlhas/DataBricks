select * from products;
select * from sales;

select  * from sales where qty >
(select avg(qty) from sales)
order by sale_id;

select sale_id,product_id,qty,discount_pct,sale_date,store,
(select AVG(unti_price) from products) As avg_unit_price
from sales order by sale_id;

select * from sales where product_id in
(select product_id from products where category = 'Stationary');


select * from sales where product_id not in
(select product_id from products where category = 'Electronics');
