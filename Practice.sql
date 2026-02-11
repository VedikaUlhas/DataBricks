create database practice;
use practice ;
-- 1. Student table
CREATE TABLE students (
    student_id INT PRIMARY KEY,
    student_name VARCHAR(50),
    dept_id INT,
    admission_date DATE
);

-- 2. Department table
CREATE TABLE departments (
    dept_id INT PRIMARY KEY,
    dept_name VARCHAR(50)
);

-- 3. Book table
CREATE TABLE books (
    book_id INT PRIMARY KEY,
    title VARCHAR(100),
    author VARCHAR(50),
    category VARCHAR(50)
);

-- 4. Borrow table (Library transaction)
CREATE TABLE borrow (
    borrow_id INT PRIMARY KEY,
    student_id INT,
    book_id INT,
    borrow_date DATE,
    return_date DATE
);

show tables;

INSERT INTO departments VALUES
(1, 'Computer Science'),
(2, 'Electronics'),
(3, 'Mechanical');

INSERT INTO students VALUES
(101, 'Arjun', 1, '2023-07-01'),
(102, 'Meena', 2, '2023-06-15'),
(103, 'Rahul', 1, '2023-08-10'),
(104, 'Priya', 3, '2023-05-12'),
(105, 'Kiran', 2, '2023-09-05');

INSERT INTO books VALUES
(201, 'DBMS Concepts', 'Korth', 'Education'),
(202, 'Operating Systems', 'Galvin', 'Education'),
(203, 'Digital Circuits', 'Morris Mano', 'Education'),
(204, 'Machine Learning', 'Murphy', 'AI'),
(205, 'Robotics Basics', 'Craig', 'Mechanical');

INSERT INTO borrow VALUES
(1, 101, 201, '2024-01-01', '2024-01-10'),
(2, 102, 203, '2024-01-05', NULL),
(3, 103, 202, '2024-01-12', '2024-01-19'),
(4, 104, 205, '2024-01-08', NULL),
(5, 101, 204, '2024-01-15', '2024-01-20');


-- Q1. List all students with their department names (Inner Join)
select s.student_id,s.student_name,s.dept_id,d.dept_name
from students s
inner join departments d
on s.dept_id=d.dept_id;

-- Q2. Show total number of books borrowed by each student. (Aggregate + Group by)
select student_id, count(book_id) from borrow group by student_id;

-- Q3. List students who have not returned books yet. (Join + return_date IS NULL)
select s.student_id,s.student_name,b.borrow_date
from students s
inner join borrow b 
on s.student_id = b.student_id
where return_date is null;


-- Q4. Show total number of borrowed books per department.  (Join Students → Borrow + Group by dept)
select s.dept_id,count(b.borrow_id)
from students s
inner join borrow b on b.student_id = s.student_id
group by dept_id;

-- OR

select s.dept_id,d.dept_name,count(b.borrow_id)
from students s
inner join borrow b on b.student_id = s.student_id
inner join departments d on d.dept_id = s.dept_id
group by dept_id;


-- Q5. List all books in ascending order of title.(Simple Order by)
select * from books order by title;


-- Q6. Find the student who borrowed the maximum number of books. (Aggregate subquery)
select s.student_id,s.student_name,count(b.borrow_id) as countOB
from students s
join borrow b on b.student_id = s.student_id
group by b.student_id
order by countOB desc
limit 1;

-- OR
select student_id,count(borrow_id) as tcount
from borrow group by student_id
order by tcount desc
limit 1;

-- Q7. Show all books borrowed in the last 7 days. (Date interval query)
select book_id , borrow_date 
from borrow where borrow_date >= curdate() - interval 7 day;

-- Q8. List departments where average borrow count per student is more than 1. (Grouping + Having)
select d.dept_id,d.dept_name
from students s
inner join borrow b on b.student_id = s.student_id
inner join departments d on d.dept_id = s.dept_id
group by dept_id
having count(b.borrow_id)/count(distinct b.student_id)>1;


-- Q9. Correlated Query → Find students who borrowed at least one book from their own department category.(e.g., CS student borrowed "DBMS Concepts")
select s.student_id,s.student_name,d.dept_name
from borrow b
inner join students s on b.student_id = s.student_id
inner join books bk on bk.book_id = b.book_id
inner join departments d on d.dept_id = s.dept_id
where d.dept_name = bk.category;

-- Q10. Correlated Subquery → List students who borrowed more books 
-- than the average borrow count per student.
select s.student_id,s.student_name,count(b.borrow_id) as bCount
from students s
inner join borrow b on b.student_id = s.student_id
group by s.student_id,s.student_name
having bCount > (count(b.borrow_id)/count(distinct b.student_id));