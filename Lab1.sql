use ltim;

CREATE TABLE Students(
StudentID INT AUTO_INCREMENT,
FirstName VARCHAR(50),
LastName VARCHAR(50),
Age INT,
PRIMARY KEY (StudentID)
);

INSERT INTO Students (FirstName,LastName,Age) VALUES ('John','Deo',18),('Jane','Smith',20);

SELECT * FROM Students;