create database if not exists acmehr;
use acmehr;

drop table if exists employe, departments,jobs;

create table departments(
dept_id int primary key,
dept_title varchar(50) not null,
location varchar(50)
);

alter table departments 
rename column dept_title to dept_name;

create table jobs(
job_id int primary key,
job_title varchar(50) not null,
min_salary decimal(10,2),
max_salary decimal(10,2)
);

create table employees (
emp_id int primary key,
emp_name varchar(50) not null,
dept_id int,
job_id int,
manager_id int,
salary decimal (10,2),
hire_date date,
foreign key (dept_id) references departments (dept_id),
foreign key (job_id) references jobs(job_id),
foreign key (manager_id) references employees(emp_id)
);

insert into departments (dept_id, dept_name,location) values
(10,'HR','Delhi'),
(20,'Engineering','Bengaluru'),
(30,'Sale','Mumbai'),
(40,'Finance','Chennai');
select * from departments;
insert into jobs (job_id,job_title,min_salary,max_salary) values
(101,'HR Generalist','30000','60000'),
(102,'HR Manager','60000','110000'),
(201,'Software Engineer','50000','150000'),
(202,'Engineering Manager','120000','220000'),
(301,'Sales Executive','30000','100000'),
(302,'Salles Manager','80000','180000'),
(401,'Accountant','35000','90000'),
(402,'Finance Manager','90000','180000');

insert into employees (emp_id,emp_name,dept_id,job_id,manager_id,salary,hire_date) values
(1,'Anita',20,202,NULL,200000,'2018-01-05'),
(2,'Ravi',20,201,1,120000,'2020-02-20'),
(3,'Meera',20,202,1,135000,'2019-07-15'),
(4,'Kunal',20,201,1,90000,'2021-03-10'),
(5,'Sana',30,302,NULL,160000,'2017-11-30'),
(6,'Vikram',30,301,5,85000,'2020-12-11'),
(7,'Asha',30,301,5,70000,'2022-05-01'),
(8,'Priya',10,102,NULL,100000,'2016-09-01'),
(9,'Ishaan',10,101,8,45000,'2021-08-18'),
(10,'Neha',10,101,8,580000,'2019-12-23'),
(11,'Arun',40,402,NULL,150000,'2015-04-01'),
(12,'Divya',40,401,11,60000,'2021-06-06');

select * from departments;
select * from jobs;
select emp_id, emp_name, dept_id,job_id,manager_id,salary 
from employees
order by dept_id,salary;

select emp_name , salary
from employees
where salary >(select avg(salary) from employees);

select emp_name ,dept_id
from employees
where dept_id in
(select dept_id from departments where location ='Bengaluru');

select emp_name,salary
from employees
where salary > any
(select salary from employees where job_id=201);

select emp_name,salary 
from employees 
where salary > all (
select salary from employees where job_id=201
);

select e.emp_name,e.dept_id,e.salary
from employees e
where e.salary >
( select avg(salary) from employees where dept_id = e.dept_id
);

select e.emp_name, e.dept_id,e.salary
from employees e
where e.salary >= ALL
(
select e2.salary from employees e2 where e2.dept_id=e.dept_id
);

select e.emp_name,
(select d.dept_name from departments d where d.dept_id=e.dept_id) as dept_name,
e.salary
from employees e;

select d.dept_name,t.avg_sal,t.min_sal,t.max_sal
from(
select dept_id,
avg(salary) as avg_sal,
min(salary) as min_sal,
max(salary) as max_sal
from employees
group by dept_id
) t
join departments d on d.dept_id =t.dept_id
order by d.dept_name;

select d.dept_name
from departments d
where exists(
select 1 from employees e where e.dept_id = d.dept_id
);

select d.dept_name
from departments d
where not exists(
select 1 from employees e where e.dept_id = d.dept_id
);

select count(*) as total_employees from employees;

select dept_id, count(*) as emp_count from employees group by dept_id;
select job_id, count(*) as job_count from employees group by job_id;

select emp_name,
(select dept_name from departments d where d.dept_id=e.dept_id) as dept_name
from employees e;

select e.emp_name,d.dept_name
from employees e join departments d on d.dept_id = e.dept_id;

select * from employees;

select e.emp_name,e.salary,e.dept_id
from employees e where e.dept_id
in (select dept_id from employees
group by dept_id order by salary desc limit 3);



