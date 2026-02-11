create database day5;
use day5;
show tables;


CREATE TABLE customers (
  customer_id INT PRIMARY KEY,
  name VARCHAR(50),
  city VARCHAR(50)
);

CREATE TABLE stores (
  store_id INT PRIMARY KEY,
  store_name VARCHAR(50),
  city VARCHAR(50)
);

CREATE TABLE products (
  product_id INT PRIMARY KEY,
  product_name VARCHAR(50),
  category VARCHAR(50),
  price DECIMAL(10,2)
);

CREATE TABLE orders (
  order_id INT PRIMARY KEY,
  customer_id INT,
  product_id INT,
  store_id INT,
  quantity INT,
  order_date DATE,
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  FOREIGN KEY (product_id) REFERENCES products(product_id),
  FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

INSERT INTO customers VALUES
(1,'Alice','Delhi'),
(2,'Bob','Mumbai'),
(3,'Charlie','Delhi'),
(4,'David','Chennai');

INSERT INTO stores VALUES
(1,'Delhi Central','Delhi'),
(2,'Mumbai Mega','Mumbai'),
(3,'Chennai Super','Chennai');

INSERT INTO products VALUES
(1,'Laptop','Electronics',80000),
(2,'Headphones','Electronics',2000),
(3,'Shoes','Apparel',2500);

INSERT INTO orders VALUES
(101,1,1,1,1,'2024-10-10'),
(102,2,2,2,2,'2024-10-11'),
(103,1,3,1,1,'2024-10-12'),
(104,3,1,3,1,'2024-10-13');

INSERT INTO customers VALUES
(1,'Alice','Delhi'),
(2,'Bob','Mumbai'),
(3,'Charlie','Delhi'),
(4,'David','Chennai');

INSERT INTO stores VALUES
(1,'Delhi Central','Delhi'),
(2,'Mumbai Mega','Mumbai'),
(3,'Chennai Super','Chennai');

INSERT INTO products VALUES
(1,'Laptop','Electronics',80000),
(2,'Headphones','Electronics',2000),
(3,'Shoes','Apparel',2500);

INSERT INTO orders VALUES
(101,1,1,1,1,'2024-10-10'),
(102,2,2,2,2,'2024-10-11'),
(103,1,3,1,1,'2024-10-12'),
(104,3,1,3,1,'2024-10-13');

select * from orders;

select 
o . *,
row_number() over (partition by customer_id order by order_date)
as order_no 
from orders o;

select * from orders;

select 
c.customer_id,
c.name,
sum(p.price * o.quantity) as total_spent,
rank() over (order by sum(p.price * o.quantity) desc ) as spend_rank
from customers c
join orders o on c.customer_id =o.customer_id
join products p on p.product_id=o.product_id
group by c.customer_id,c.name;

insert into orders values
(105,4,1,1,1,'2024-10-10'),
(108,4,3,1,1,'2024-10-12');

select 
c.customer_id,
c.name,
sum(p.price * o.quantity) as total_spent,
dense_rank() over (order by sum(p.price * o.quantity) desc ) as spend_rank
from customers c
join orders o on c.customer_id =o.customer_id
join products p on p.product_id=o.product_id
group by c.customer_id,c.name;

select 
c.customer_id,
c.name,
sum(p.price * o.quantity) as total_spent,
row_number() over (order by sum(p.price * o.quantity) desc ) as spend_rank
from customers c
join orders o on c.customer_id =o.customer_id
join products p on p.product_id=o.product_id
group by c.customer_id,c.name;

select 
order_id,
customer_id,
order_date,
lag(order_date) over (partition by customer_id )
as previous_order_date
from orders;

select 
order_id,
customer_id,
product_id,
lead(product_id) over (partition by customer_id order by order_date)
as next_product
from orders;
select product_id,product_name,category,price,
rank() over (partition by category order by price desc) 
as rnk from products;

select * from 
(select product_id,product_name,category,price,
rank() over (partition by category order by price desc) 
as rnk from products
) t
where rnk =1;

select 
product_id,
product_name,
price,
case 
when price>50000 then 'Premium'
when price<5000 then 'Budget'
else 'Standard'
end
as price_category
from products;

use cricketdb;

select* from players;


select * from(
select full_name, country, dob,
rank() over(partition by country order by dob desc) as rnk
from players
) t
where rnk  =1;

select full_name,country,dob
from players p
where dob = (
select max(dob) from players where country=p.country);

select p1.full_name,p1.country,p1.dob from players p1
where p1.dob = (select max(p2.dob) from
 players p2 where p1.country=p2.country);
 
 
select * from orders;

select order_id,quantity,order_date from orders
where order_date =
(select max(order_date) from orders);

select customer_id,order_id,quantity,order_date 
from orders o
where order_date = 
(select max(order_date) from orders
 where customer_id=o.customer_id)
 order by customer_id;
 
 DELIMITER //
CREATE FUNCTION total_price(qty INT, price DECIMAL(10,2))
RETURNS DECIMAL(10,2)
DETERMINISTIC
BEGIN
    RETURN qty * price;
END //
DELIMITER ;


SELECT total_price(quantity, order_id) FROM orders;

DELIMITER //
CREATE PROCEDURE get_customer_orders(IN cid INT)
BEGIN
    SELECT order_id, product_id, quantity
    FROM orders
    WHERE customer_id = cid;
END //
DELIMITER ;

CALL get_customer_orders(3);

DELIMITER//
CREATE FUNCTION tds_deduction(sal decimal(10,2))
RETURNS DECIMAL (10,2)
DETERMINISTIC
BEGIN
	RETURN sal*0.1;
END//
DELIMITER;


CREATE TABLE order_audit (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



delimiter $$

create trigger trg_order_insert
after insert on orders
for each row
begin
	insert into order_audit(order_id) values (new.order_id);
end $$

delimiter ;

select * from order_audit;
insert into order_audit  values (107,NULL,NULL);

delimiter $$
create trigger chech_price
before insert on products
for each row
begin
	if new.price <=0 then
    signal sqlstate '45000'
    set message_text = 'Price must be greater than 0';
    end if;
end $$
delimiter ;


desc products;
insert into products values (5,'ABC','Mobile',0);

