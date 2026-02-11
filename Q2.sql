use ltim;
show tables;

select * from emp;
select * from dept;

select *
from emp e
JOIN dept d 
on e.deptno=d.deptno;

select e.ename,d.dname
from emp e
INNER JOIN dept d 
on e.deptno=d.deptno;

set sql_safe_updates=0;
update emp set deptno=70 where empno = 7777;

select *
from emp e
left join dept d
ON e.deptno=d.deptno;

select e.ename,d.dname
from emp e
left join dept d
ON e.deptno=d.deptno;

select e.ename ,d.dname
FROM emp e
right join dept d
ON e.deptno=d.deptno;

select e.ename,d.dname 
FROM emp e
lEFT JOIN dept d
on e.deptno=d.deptno
UNION 
select e.dname, d.dname
FROM emp e
right join 