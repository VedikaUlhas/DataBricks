use ltim;

CREATE TABLE Departments(
DepartmentID INT AUTO_INCREMENT,
DepartmentName VARCHAR(100) NOT NULL,
UNIQUE (DepartmentName),
PRIMARY KEY (DepartmentID)
);

DROP TABLE employees;

CREATE TABLE Employees(
EmployeeID INT AUTO_INCREMENT,
FirstName VARCHAR(50) NOT NULL,
LastName VARCHAR(50) NOT NULL,
DepartmentID INT,
PRIMARY KEY (EmployeeID),
FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentId)
);

SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE TABLE_SCHEMA = 'ltim' AND TABLE_NAME = 'Employees';

CREATE TABLE Products(
ProductID INT AUTO_INCREMENT,
ProductName VARCHAR(100) NOT NULL,
Price DECIMAL (10,2) DEFAULT 0.00,
Stock INT CHECK(Stock >= 0),
PRIMARY KEY (ProductID)
);

SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE TABLE_SCHEMA = 'ltim' AND TABLE_NAME = 'Products';

INSERT INTO Departments (DepartmentName) VALUES ('HR'),('IT');

INSERT INTO Employees (FirstName,LastName, DepartmentID) VALUES('Charlie','Davis',1),('Dana','Evans',2);

INSERT INTO Products (ProductName,Price, Stock) VALUES ('Laptop',1500.00,10),('Mouse',25.00,50);

select * from stuents;

