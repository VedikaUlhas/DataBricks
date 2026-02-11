create database retailkart;
use retailkart;

create table customer (
customer_id int primary key,
name varchar(50),
city varchar(50)
);

create table stores(
store_id int primary key,
store_name varchar(50),
city varchar(50)
);

create table products(
product_id int primary key,
product_name varchar(50),
category varchar(50),
price decimal(10,2)
);

create table orders (
order_id int primary key,
customer_id int,
product_id int,
store_id int,
quantity int,
order_date int,
foreign key (customer_id) references customer(customer_id),
foreign key (product_id) references products(product_id),
foreign key (store_id) references stores(store_id)
);

insert into customer values
(1,'Alice','Delhi'),
(2,'Bob','Mumbai'),
(3,'Charlie','Delhi'),
(4,'David','Chennai');

insert into stores values
(1,'Delhi Central','Delhi'),
(2,'Mumbai Mega','Mumbai'),
(3,'Chennai Super','Chennai');

insert into products values 
(1,'Laptop','Electronics',80000),
(2,'Headphones','Electronics',2000),
(3,'Shoes','Apparel',2500);

alter table orders modify order_date date; 

insert into orders values
(101,1,1,1,1,'2024-10-10'),
(102,2,2,2,2,'2024-10-11'),
(103,1,3,1,1,'2024-10-12'),
(104,3,1,3,1,'2024-10-13');

select * from customer;
select * from orders;
select * from products;
select * from stores;

select o.order_id,c.name,o.order_date
from orders o
inner join customer c  on o.customer_id = c.customer_id;

select c.name,o.order_id,o.order_date
from customer c
left join orders o on c.customer_id=o.customer_id;

select s.store_name,o.order_id,o.customer_id
from orders o
right join stores s on o.store_id = s.store_id;

select * from stores;

select c.customer_id, c.name, o.order_id,o.order_date
from customer c
left join orders o on c.customer_id=o.customer_id
union
select c.customer_id, c.name, o.order_id,o.order_date
from customer c
right join orders o on c.customer_id=o.customer_id;

create table employees(
emp_id int primary key,
emp_name varchar(50),
manager_id int
);

insert into employees values
(1,'John',NULL),
(2,'Maya',1),
(3,'Karan',1),
(4,'Rita',2);

select e.emp_name as Employee, m.emp_name as Manager
from employees e
left join employees m on e.manager_id=m.emp_id;

select p.product_name, s.store_name
from products p
cross join stores s;

select c.name,p.product_name,s.store_name,o.quantity
from orders o
join customer c on c.customer_id=o.customer_id
join products p on p.product_id=o.product_id
join stores s on s.store_id = o.store_id;

select c.name,p.product_name,s.store_name,o.quantity
from orders o
join customer c on c.customer_id=o.customer_id
join products p on p.product_id=o.product_id
join stores s on s.store_id = o.store_id
where s.city='Delhi';

select s.store_name, sum(o.quantity) as total_sold
from orders o
join stores s on o.store_id = s.store_id
group by s.store_name;

explain select * from orders o join customer c on o.customer_id=c.customer_id;

select c.customer_id,c.name 
from customer c
left join orders o on c.customer_id=o.customer_id
where order_id is null;

select p.product_id, p.product_name 
from products p
left join orders o on o.product_id = p.product_id
where store_id is null;

use retailkart;
select c.name,c.city,p.product_name,sum(o.quantity)
from orders o
inner join products p on p.product_id=o.product_id
inner join customer c on c.customer_id=o.customer_id
group by c.name,c.city,p.product_name;




