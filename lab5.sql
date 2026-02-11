use ltim;
show tables;

select * from employee;
desc employee;

INSERT into employee(FirstName,LastName,Salary,DateOfJoining,TimeOfJoining,Notes)
VALUES ('Allen','Ward',60000.00,'2023-12-16','11:00:00','Junior Engineer');

ALTER TABLE employee ADD date_of_birth DATE;

UPDATE employee SET date_of_birth = '1985-01-15' WHERE employeeId = 1; 
UPDATE employee SET date_of_birth = '1990-05-20' WHERE employeeId = 2; 
UPDATE employee SET date_of_birth = '1988-03-10' WHERE employeeId = 3; 

SELECT FirstName, TIMESTAMPDIFF (YEAR, date_of_birth, CURDATE()) AS age FROM employee;

select FirstName from employee where extract(YEAR FROM date_of_birth) = 1980;

select FirstName, DATE_ADD(date_of_birth, INTERVAL 60 YEAR) as retireDate from employee;

SELECT FirstName, Lastname, dateofjoining from employee where dateofjoining >= date_sub(curdate(),INTERVAL 5 YEAR);

SELECT DAYOFWEEK(date_of_birth) as DAY , MONTHNAME(date_of_birth) as month from employee;

SELECT FirstName, date_of_birth ,
DATEDIFF(
	CASE
		WHEN DATE_FORMAT(date_of_birth,'%m-%d') >= DATE_FORMAT(CURDATE(),'%m-%d')
        THEN CONCAT(YEAR(CURDATE()),'-',DATE_FORMAT(date_of_birth,'%m-%d'))
        ELSE CONCAT(YEAR(CURDATE()+1),'-',DATE_FORMAT(date_of_birth,'%m-%d'))
        END,
        curdate()
) AS days_until_next_birthday
FROM employee;

Select FirstName,dayofweek(date_of_birth), date_of_birth from employee where dayofweek(date_of_birth) =7 OR dayofweek(date_of_birth)=1;

SELECT AVG(TIMESTAMPDIFF(YEAR,date_of_birth,curdate())) as avg_age from employee;