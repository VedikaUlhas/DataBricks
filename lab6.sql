DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS products;

CREATE TABLE products(
product_id INT PRIMARY KEY,
product_name VARCHAR(50),
category VARCHAR(30),
unti_price DECIMAL(10,2)
);

desc products;

CREATE TABLE sales(
sale_id INT PRIMARY KEY,
product_id INT,
qty INT,
discount_pct DECIMAL(5,2),
sale_date DATE,
store VARCHAR (30),
FOREIGN KEY (product_id)
REFERENCES 
products(product_id)
);

desc sales;

INSERT INTO products (product_id,product_name,category,unti_price)
VALUES (1,'Notebook A5','Stationary',120.00),
(2,'Gel Pen','Stationary',20.00),
(3,'Backpack','Bags',1800.00),
(4,'Water Bottle','Accessories',350.00),
(5,'Laptop','Electronics',65000.00);

INSERT INTO sales (sale_id,product_id,qty,discount_pct,sale_date,store)
values (101,1,5,0.00,'2025-01-05','Hyderabad'),
(102,2,10,0.10,'2025-01-05','Hyderabad'),
(103,3,2,0.05,'2025-01-06','Bengaluru'),
(104,4,1,0.00,'2025-01-06','Bengaluru'),
(105,5,1,0.15,'2025-01-07','Chennai'),
(106,1,3,0.00,'2025-01-08','Chennai'),
(107,2,4,0.00,'2025-01-08','Hyderabad'),
(108,3,1,0.20,'2025-01-09','Chennai');