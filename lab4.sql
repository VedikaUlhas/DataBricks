
use ltim;

select * from students;

ALTER TABLE students ADD city VARCHAR(50);

UPDATE students SET city = 'NEW YORK' WHERE StudentID =1;
UPDATE students SET city = 'Los Angeles' WHERE StudentID =2;
UPDATE students SET city = 'Chicago' WHERE StudentID = 3;
UPDATE students SET city = 'San Francisco' WHERE StudentID =4;
UPDATE students SET city = 'Boston' WHERE StudentID =5;

SELECT * FROM students WHERE age >20;

SELECT * FROM students WHERE city = 'New York';

SELECT * FROM students WHERE FirstName LIKE 'A%';

SELECT * FROM students WHERE city = 'Los Angeles' OR city ='San Francisco';

SELECT * FROM students wHeRe city != 'Boston';

SELECT * FROM students WHERE age BETWEEN 20 AND 22;

SELECT * FROM students WHERE city IN ('NEW YORK','Chicago');
