show databases;

use ltim;

CREATE TABLE EMpLoyee(
EmployeeID INT AUTO_INCREMENT,
FirstName VARCHAR(50),
LastName VARCHAR(50),
Salary DECIMAL (10,2),
DateOfJoining DATE,
TimeOfJoining TIME,
Notes TEXT,
PRIMARY KEY (EmployeeID)
);

INSERT INTO Employee (FirstName,LastName,Salary, DateOfJoining,TimeOfJoining,Notes)
 VALUES ('Alice','Brown',75000.50,'2023-06-15','09:00:00','Senior Developer'),
 ('Bob','Johnson',50000.00,'2024-01-20','10:30:00','Junior Developer');
 
 select * from Employee;
 
 create table Courses(
 CourseID INT AUTO_INCREMENT,
 CourseName VARCHAR(100),
 Credits INT,
 PRIMARY KEY (CourseID)
 );
 
 DESCRIBE Courses;
 
 ALTER TABLE Courses ADD Instructor VARCHAR(50);
  DESCRIBE Courses;
  
ALTER TABLE  Courses MODIFY Credits DECIMAL(3,1);
DESCRIBE Courses;

TRUNCATE TABLE Courses;

DROP TABLE Courses;