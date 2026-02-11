show tables;

create table employees1 (
employee_id int primary key,
name varchar(50),
department varchar(50)
);

create table salaies (
employee_id int,
salary decimal(10,2),
foreign key(employee_id) references employees1(employee_id)
);

insert into employees1 values
(1,'Alice','HR'),
(2,'Bob','Engineering'),
(3,'Charlie','Finance');

insert into salaies values
(1,50000.00),
(2,70000.00),
(3,60000.00);

select name , department 
from employees1
where employee_id in
(select employee_id from salaies
where salary>
(select avg(salary) from salaies)
);


