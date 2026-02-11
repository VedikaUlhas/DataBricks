show databases;

use ltim;
show tables;

create table if not exists product (
productID int unsigned not null auto_increment,
 productCode char(3) not null default '',
 name varchar(30) not null default ' ',
 quantity int unsigned not null default 0, 
 price decimal (7,2) not null default 99999.99,
 primary key (productId));
 
 select * from product;
 
 describe product;
 
 insert into product values (1001,'PEN','Pen red',5000,1.23);
 
 select * from product;
 
 insert into product values
 (NULL,'PEN','Pen blue',8000,1.25),
 (NULL,'PEN','Pen Black',2000,1.25);
 
 select * from product;
 
 insert into product (productCode,name,quantity,price) 
 values ('PCl','Apsara',5000,'2'),
 ('BOK','Science',4000,3);
 
 insert into product (name,quantity,price) values ('Camlin',2000,1.1);
 
 select * from product;
 
 delete from product where productId = 1006;
 
 insert into product (productCode, name, quantity, price) values ('Ers','Domes',3000,2);
 
 set sql_safe_updates=0;
 delete from product where price =3;
 select * from product;
 
DROP TABLE IF EXISTS emp;
DROP TABLE IF EXISTS dept;

 	CREATE TABLE dept (
  deptno DECIMAL(2,0) NOT NULL,
  dname VARCHAR(14) DEFAULT NULL,
  loc VARCHAR(13) DEFAULT NULL,
  PRIMARY KEY (deptno)
);

desc dept;

-- Employee table
CREATE TABLE emp (
  empno DECIMAL(4,0) NOT NULL,
  ename VARCHAR(10) DEFAULT NULL,
  job VARCHAR(9) DEFAULT NULL,
  mgr DECIMAL(4,0) DEFAULT NULL,
  hiredate DATE DEFAULT NULL,
  sal DECIMAL(7,2) DEFAULT NULL,
  comm DECIMAL(7,2) DEFAULT NULL,
  deptno DECIMAL(2,0) DEFAULT NULL,

  PRIMARY KEY (empno),

  -- foreign key linking to dept table
  CONSTRAINT fk_dept FOREIGN KEY (deptno)
        REFERENCES dept(deptno)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

-- Insert departments first
INSERT INTO dept VALUES ('10','ACCOUNTING','NEW YORK');
INSERT INTO dept VALUES ('20','RESEARCH','DALLAS');
INSERT INTO dept VALUES ('30','SALES','CHICAGO');
INSERT INTO dept VALUES ('40','OPERATIONS','BOSTON');

-- Insert employees
INSERT INTO emp VALUES ('7369','SMITH','CLERK','7902','1980-12-17','800.00',NULL,'20');
INSERT INTO emp VALUES ('7499','ALLEN','SALESMAN','7698','1981-02-20','1600.00','300.00','30');
INSERT INTO emp VALUES ('7521','WARD','SALESMAN','7698','1981-02-22','1250.00','500.00','30');
INSERT INTO emp VALUES ('7566','JONES','MANAGER','7839','1981-04-02','2975.00',NULL,'20');
INSERT INTO emp VALUES ('7654','MARTIN','SALESMAN','7698','1981-09-28','1250.00','1400.00','30');
INSERT INTO emp VALUES ('7698','BLAKE','MANAGER','7839','1981-05-01','2850.00',NULL,'30');
INSERT INTO emp VALUES ('7782','CLARK','MANAGER','7839','1981-06-09','2450.00',NULL,'10');
INSERT INTO emp VALUES ('7788','SCOTT','ANALYST','7566','1982-12-09','3000.00',NULL,'20');
INSERT INTO emp VALUES ('7839','KING','PRESIDENT',NULL,'1981-11-17','5000.00',NULL,'10');
INSERT INTO emp VALUES ('7844','TURNER','SALESMAN','7698','1981-09-08','1500.00','0.00','30');
INSERT INTO emp VALUES ('7876','ADAMS','CLERK','7788','1983-01-12','1100.00',NULL,'20');
INSERT INTO emp VALUES ('7900','JAMES','CLERK','7698','1981-12-03','950.00',NULL,'30');
INSERT INTO emp VALUES ('7902','FORD','ANALYST','7566','1981-12-03','3000.00',NULL,'20');
INSERT INTO emp VALUES ('7934','MILLER','CLERK','7782','1982-01-23','1300.00',NULL,'10');

insert into emp values ('7777','STEVE','CLEARK','7782','1982-01-23','1300.00',null,'50');
-- Cannot insert above enrty because 50 is not in dept tables deptid
select * from emp;

insert into dept values ('50','DATA ENGG','LOS ANGELS');
insert into emp values ('7777','STEVE','CLEARK','7782','1982-01-23','1300.00',null,'50');
select * from emp;

delete from dept where deptno =50;
select * from emp;

insert into dept values ('50','DATA ENGG','LOS ANGELS');
update emp set deptno =50 where empno=7777;
select * from emp;

update dept set deptno = 60 where dname = 'DATA ENGG';

alter table emp drop constraint fk_dept;
alter table emp drop primary key;
desc emp;

select * from information_schema.KEY_COLUMN_USAGE;

alter table emp
add aadhar_number varchar(12),
add constraint uq_emp_aadhar UNIQUE (aadhar_number);

desc emp;

-- DROP if exist to allow re-run
DROP TABLE IF EXISTS bowling;
DROP TABLE IF EXISTS batting;
DROP TABLE IF EXISTS players;

