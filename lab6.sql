
select s.sale_id, s.store,p.product_name 
from sales s 
JOIN products p on p.product_id = s.product_id
where p.category in(
select category from products where 
category in ('Bags','Electronics')
)
order by s.sale_id;

select s.sale_id,s.store,p.product_name,p.category,p.unti_price,
(select avg(unti_price) from products x where x.category =
p.category) as category_avg_price 
from sales s
join products p on p.product_id = s.product_id
order by s.sale_id;

select s.sale_id,s.sale_date,s.store,p.product_name,
(s.qty * p.unti_price *(1 - s.discount_pct)) as net_amount
from sales s
join products p on p.product_id = s.product_id
where s.sale_date >= (
select min(sale_date) from 
sales where sale_date >= '2025-01-06'
)
order by s.sale_date,s.sale_id;

select s.sale_id,p.product_name,p.category,p.unti_price
from sales s
join products p on p.product_id =s.product_id
where exists(select 1 from products p2 where p2.category = p.category and p2.unti_price >3000)
order by s.sale_id;

select s.sale_id,s.store,p.product_name,
(s.qty * p.unti_price *(1-s.discount_pct)) as net_amount
from sales s
join products p on p.product_id =s.product_id
where (s.qty * p.unti_price *(1-s.discount_pct))>
(select avg (s2.qty * p2.unti_price *(1-s2.discount_pct))
from sales s2
join products p2 on
p2.product_id =s2.product_id
where s2.store = s.store)
order by s.store,net_amount desc;
