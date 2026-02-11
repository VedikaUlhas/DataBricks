-- Create duplicate table emp2 using emp
create table emp2 as select  * from emp;


create table emp_10
as select * from emp where deptno = 10; 

create table emp_skeleton
as select * from emp where 1=2;

select * from emp_10;



select * from emp_10;
alter table emp_10 add column bonus decimal(10,2);

insert into emp_10 values
(3333,'AK','DATA ENGG',7902,'2025-11-20',9999,100,10,NULL,20);

-- error due to unmatched no of cols
select * from emp 
union 
select * from emp_10;

select * from emp 
union all
select empno,ename,job,mgr,hiredate,sal,comm,
deptno,aadhar_number
 from emp_10;

USE cricketdb;

select * from players;

select full_name, dob from players where dayofweek(dob) = 1
OR dayofweek(dob) =7;

use ltim;

select * from sales;
select * from products;

select * from sales
where sale_date >= curdate() - interval 7 year;


use ltim;
select * from emp;
select * from dept;

-- updatable views
create view v_emp_dept
as
select empno,ename,job,sal,emp.deptno,dname,loc
from emp
join dept on emp.deptno=dept.deptno;

desc v_emp_dept;
select * from v_emp_dept;

set sql_safe_updates =0;
update emp set sal =8888 where empno=7782; 

update v_emp_dept set sal =9999 where empno=7782; 
select * from emp;


create or replace view v_emp_dept
as
select  empno,ename,job,sal,emp.deptno,dname,loc
from emp
join dept on emp.deptno=dept.deptno
where emp.deptno=10
with check option;

select * from v_emp_dept;
update v_emp_dept set sal =9999 where empno=7782; 

select * from emp;

-- unupdatable views
create or replace view v_emp_dept
as
select distinct  empno,ename,job,sal,emp.deptno,dname,loc
from emp
join dept on emp.deptno=dept.deptno;

use cricketdb;

-- Creating user in db

create user
'cricket_admin'@'localhost'
identified by 'P@55w0rd';

create user 'trainer'@'%'
identified by 'P@55w0rd';

-- granting privileges
-- full privileges
grant all privileges on cricket_db.* to 'cricket_admin'@'localhost';

-- read only user for trainees
grant select on cricket_db.* to 'trainer'@'%';

-- grant specific permissions
grant select,insert, update on players to 'trainer'@'%';

-- revoke privileges
revoke insert, update on players from 'trainer'@'%';

-- show privileges
show grants for 'trainer'@'%';

use ltim;
select * from emp;

create index idx_ename
on emp(ename);

show index from emp;

select * from information_schema.statistics;