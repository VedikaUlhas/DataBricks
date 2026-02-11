use ltim;
show tables;
select * from prod;
select * from customers;
select * from orders;

select o.order_id,c.name as customerName ,p.product_name,o.quantity,o.order_date
from orders o
inner join customers c on c.customer_id=o.customer_id
inner join prod p on o.product_id= p.product_id;

select c.name as customerName, o.order_id,o.quantity
from customers c
left join orders o 
on c.customer_id=o.customer_id;

select c.name as customerName,p.product_name
from customers c
left join prod p on c.customer_id=p.product_id
union
select c.name as customerName,p.product_name
from customers c
right join prod p on c.customer_id=p.product_id;


select c.name 
from customers c 
left join orders o
on c.customer_id=o.customer_id 
where o.order_id is null;

select o1.order_id,o2.order_id 
from orders o1
inner join orders o2 
on o1.customer_id = o2.customer_id and o1.order_id != o2.order_id;


select c.name as customerName, sum(o.quantity * p.price) as totalRevenue
from customers c
inner join orders o on c.customer_id=o.customer_id
inner join prod p  on p.product_id = o.product_id
group by c.name; 

use cricketdb;

select p.full_name , b.runs
from players p
join batting b 
on p.player_id = b.player_id
AND b.format = 'ODI'
where b.runs > (select avg(runs) from batting where format ='ODI');

select full_name from players 
where player_id in (select player_id from bowling);

select * from players;

select full_name 
from players
where (player_id, jersey_number)=(
select player_id,jersey_number
from players
order by jersey_number desc 
limit 1
);

select p.full_name from players p
join batting b where 
b.runs>(select max(runs) from batting) AND  
b.batting_avg>(select max(runs) from batting);

select p.full_name,b.runs,b.batting_avg
from players p
join batting b
on p.player_id=b.player_id
where (b.runs,b.batting_avg) = (
select runs, batting_avg
from batting
order by batting_avg DESC
LIMIT 1
);

use ltim;
show tables;

select * from emp;

select e1.ename,e2.ename as manager from emp e1 
join emp e2 on e1.mgr=e2.empno;

-- manager whose salary is greater than avg salary of org
select e1.ename,e2.ename as manager from emp e1 
join emp e2 on e1.mgr=e2.empno
where e2.sal > (select avg(sal) from emp);

select ename,sal from emp where (ename,sal) in
(select ename,sal from emp order by sal desc limit 3)
order by sal limit 1;