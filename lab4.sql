use ltim;
select * from customers;
select * from prod;
show tables;

select c.name as customerName, o.order_id 
from customers c
left join orders o
on c.customer_id = o.customer_id;

select c.name, p.product_name 
from customers c
left join products p
on c.customer_id=p.product_id
union
select c.name, p.product_name 
from customers c
right join products p
on c.customer_id=p.product_id;

select c.name as customerName
from customers c
left join orders o 
on c.customer_id = o.customer_id
where o.order_id is null;

select o1.order_id ,o1.customer_id 
from orders o1
join orders o2
on o1.customer_id=o2.customer_id
AND o1.product_id=o2.product_id
AND o1.order_date=o2.order_date
AND o1.order_id != o2.order_id;
 
 select * from prod;
 select c.name as cName, sum(o.quantity * p.price)
 from orders o
 inner join customers c
 on c.customer_id=o.customer_id
 inner join prod p
 on o.product_id=p.product_id
 group by c.name;
 
 select * from prod;
 select c.name as cName, sum(o.quantity * p.price) as sum
 from orders o
 inner join customers c
 on c.customer_id=o.customer_id
 inner join prod p
 on o.product_id=p.product_id
 group by c.name
 having sum(o.quantity*p.price)>2000;
 