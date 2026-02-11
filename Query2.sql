create database cricketDB;
use cricketDB;

show databases;

-- Players table: player_id is a simple 4-digit integer
CREATE TABLE players (
    player_id INT NOT NULL,                -- 4-digit id (e.g. 1001)
    full_name VARCHAR(100) NOT NULL,
    country VARCHAR(50),
    role ENUM('Batsman','Bowler','All-rounder','Wicket-keeper') DEFAULT 'Batsman',
    batting_style VARCHAR(30),
    bowling_style VARCHAR(50),
    dob DATE,
    debut_date DATE,
    active TINYINT(1) DEFAULT 1,
    jersey_number SMALLINT,
    PRIMARY KEY (player_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

desc players;

-- Batting stats table: composite PK (player_id, format)
CREATE TABLE batting (
    player_id INT NOT NULL,
    format ENUM('Test','ODI','T20') NOT NULL DEFAULT 'ODI',
    matches SMALLINT DEFAULT 0,
    innings SMALLINT DEFAULT 0,
    runs INT DEFAULT 0,
    highest INT DEFAULT 0,
    batting_avg DECIMAL(5,2) DEFAULT 0.00,
    strike_rate DECIMAL(6,2) DEFAULT 0.00,
    hundreds SMALLINT DEFAULT 0,
    fifties SMALLINT DEFAULT 0,
    not_outs SMALLINT DEFAULT 0,
    PRIMARY KEY (player_id, format),
    CONSTRAINT fk_batting_player FOREIGN KEY (player_id) REFERENCES players(player_id)
      ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

desc batting;

-- Bowling stats table: composite PK (player_id, format)
CREATE TABLE bowling (
    player_id INT NOT NULL,
    format ENUM('Test','ODI','T20') NOT NULL DEFAULT 'ODI',
    matches SMALLINT DEFAULT 0,
    innings SMALLINT DEFAULT 0,
    wickets INT DEFAULT 0,
    best_figures VARCHAR(10),            -- e.g. '5/27'
    bowling_avg DECIMAL(5,2) DEFAULT 0.00,
    economy DECIMAL(4,2) DEFAULT 0.00,
    strike_rate DECIMAL(6,2) DEFAULT 0.00,
    five_wickets SMALLINT DEFAULT 0,
    ten_wickets SMALLINT DEFAULT 0,
    PRIMARY KEY (player_id, format),
    CONSTRAINT fk_bowling_player FOREIGN KEY (player_id) REFERENCES players(player_id)
      ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

desc bowling;

-- Insert 20 players (IDs 1001..1020)
INSERT INTO players (player_id, full_name, country, role, batting_style, bowling_style, dob, debut_date, active, jersey_number) VALUES
(1001, 'Rohit Sharma',     'India',        'Batsman',        'Right-hand', 'Right-arm offbreak', '1987-04-30','2007-06-23', 1, 45),
(1002, 'Virat Kohli',      'India',        'Batsman',        'Right-hand', 'None',              '1988-11-05','2008-08-18', 1, 18),
(1003, 'Jasprit Bumrah',   'India',        'Bowler',         'Right-hand', 'Right-arm fast',    '1993-12-06','2016-01-03', 1, 93),
(1004, 'Joe Root',         'England',      'Batsman',        'Right-hand', 'None',              '1990-12-30','2012-12-10', 1, 66),
(1005, 'Ben Stokes',       'England',      'All-rounder',    'Left-hand',  'Right-arm fast',    '1991-06-04','2011-08-10', 1, 55),
(1006, 'Pat Cummins',      'Australia',    'Bowler',         'Right-hand', 'Right-arm fast',    '1993-05-08','2011-11-17', 1, 30),
(1007, 'David Warner',     'Australia',    'Batsman',        'Left-hand',  'None',              '1986-10-27','2009-01-11', 0, 31),
(1008, 'Kane Williamson',  'New Zealand',  'Batsman',        'Right-hand', 'Off-spin',          '1990-08-08','2010-11-04', 1, 22),
(1009, 'Trent Boult',      'New Zealand',  'Bowler',         'Right-hand', 'Left-arm fast',     '1989-07-22','2011-12-09', 1, 18),
(1010, 'Shaheen Afridi',   'Pakistan',     'Bowler',         'Left-hand',  'Left-arm fast',     '2000-04-06','2018-09-15', 1, 10),
(1011, 'Babar Azam',       'Pakistan',     'Batsman',        'Right-hand', 'None',              '1994-10-15','2015-05-14', 1, 56),
(1012, 'Shakib Al Hasan',  'Bangladesh',   'All-rounder',    'Left-hand',  'Left-arm spin',     '1987-03-24','2006-08-06', 1, 75),
(1013, 'Tamim Iqbal',      'Bangladesh',   'Batsman',        'Left-hand',  'None',              '1989-03-20','2007-02-09', 0, 28),
(1014, 'Kusal Mendis',     'Sri Lanka',    'Batsman',        'Right-hand', 'None',              '1995-02-02','2015-01-03', 1, 20),
(1015, 'Wanindu Hasaranga', 'Sri Lanka',   'All-rounder',    'Right-hand', 'Right-arm legbreak','1997-02-28','2019-07-18', 1, 49),
(1016, 'Quinton de Kock',  'South Africa', 'Wicket-keeper',  'Left-hand',  'None',              '1992-12-17','2013-12-17', 1, 12),
(1017, 'Kagiso Rabada',    'South Africa', 'Bowler',         'Right-hand', 'Right-arm fast',    '1995-05-25','2014-11-05', 1, 25),
(1018, 'Jason Holder',     'West Indies',  'All-rounder',    'Right-hand', 'Right-arm fast',    '1991-11-05','2013-02-13', 1, 98),
(1019, 'Nicholas Pooran',  'West Indies',  'Wicket-keeper',  'Left-hand',  'None',              '1995-10-02','2016-01-23', 1, 29),
(1020, 'Andre Russell',    'West Indies',  'All-rounder',    'Right-hand', 'Right-arm fast',    '1988-04-29','2011-06-11', 1, 12);

select * from players;

-- Insert ODI batting stats for those 20 players
INSERT INTO batting (player_id, format, matches, innings, runs, highest, batting_avg, strike_rate, hundreds, fifties, not_outs) VALUES
(1001,'ODI',260,243,10000,264,48.50,89.70,29,64,18),
(1002,'ODI',300,287,12000,183,56.32,92.30,43,58,22),
(1003,'ODI',90,82,350,58,15.20,82.40,0,1,4),
(1004,'ODI',160,150,6850,133,50.30,88.40,18,39,12),
(1005,'ODI',194,180,5500,102,42.10,89.20,6,31,9),
(1006,'ODI',77,72,800,38,18.30,75.20,0,0,8),
(1007,'ODI',149,138,8500,179,46.20,95.10,22,46,10),
(1008,'ODI',151,140,7000,133,52.10,85.60,21,34,7),
(1009,'ODI',79,74,500,44,12.20,70.50,0,0,2),
(1010,'ODI',45,40,300,36,13.20,85.00,0,0,3),
(1011,'ODI',125,118,6000,125,56.20,89.90,21,27,6),
(1012,'ODI',245,230,7000,120,40.10,88.00,6,42,11),
(1013,'ODI',150,140,7800,128,35.30,83.70,12,38,5),
(1014,'ODI',136,128,4500,122,34.90,86.40,6,25,8),
(1015,'ODI',54,46,1500,80,32.10,110.40,0,8,3),
(1016,'ODI',140,130,6000,178,45.20,96.20,15,32,9),
(1017,'ODI',120,110,400,48,12.50,78.40,0,0,1),
(1018,'ODI',110,100,2500,95,33.20,84.50,1,15,7),
(1019,'ODI',78,69,2200,75,43.10,110.10,1,18,6),
(1020,'ODI',85,70,2000,88,30.10,140.50,2,8,10);

select * from batting;

-- Insert ODI bowling stats for those 20 players
INSERT INTO bowling (player_id, format, matches, innings, wickets, best_figures, bowling_avg, economy, strike_rate, five_wickets, ten_wickets) VALUES
(1001,'ODI',260,240,30,'3/22',50.00,5.20,120.00,0,0),
(1002,'ODI',300,285,8,'2/15',62.50,5.10,200.00,0,0),
(1003,'ODI',90,88,200,'5/27',21.10,4.60,29.00,5,0),
(1004,'ODI',160,158,30,'3/20',40.00,4.80,90.00,0,0),
(1005,'ODI',194,180,150,'6/43',29.40,4.90,40.00,3,0),
(1006,'ODI',77,74,250,'7/31',24.80,4.50,22.00,8,0),
(1007,'ODI',149,140,0,NULL,0.00,0.00,0.00,0,0),
(1008,'ODI',151,140,30,'4/22',40.60,4.70,95.00,0,0),
(1009,'ODI',79,78,240,'4/18',25.40,4.60,30.00,0,0),
(1010,'ODI',45,43,180,'6/29',23.80,4.75,29.00,3,0),
(1011,'ODI',125,120,10,'2/21',41.00,4.90,140.00,0,0),
(1012,'ODI',245,230,260,'7/36',29.50,4.80,38.00,7,0),
(1013,'ODI',150,140,0,NULL,0.00,0.00,0.00,0,0),
(1014,'ODI',136,128,5,'2/18',55.00,5.30,90.00,0,0),
(1015,'ODI',54,50,120,'5/20',22.50,4.20,22.00,4,0),
(1016,'ODI',140,130,0,NULL,0.00,0.00,0.00,0,0),
(1017,'ODI',120,115,210,'6/35',24.00,4.60,25.00,6,0),
(1018,'ODI',110,105,140,'5/29',26.40,4.75,36.00,4,0),
(1019,'ODI',78,74,0,NULL,0.00,0.00,0.00,0,0),
(1020,'ODI',85,70,70,'5/23',25.30,6.20,29.00,2,0);

select * from bowling;

select count(*) from players;
select * from players limit 5;
select full_name from players where role = 'Batsman';
select full_name from players where role='Bowler' order by full_name desc;

select country , count(*) from players group by country;

select country , count(*) as player_count 
from players where role = 'Bowler' 
group by country 
order by player_count;

select country,role,count(*) as player_count from players group by country, role order by player_count desc;

select country,role,count(*) as player_count from players where role='all-rounder' group by country, role order by player_count desc;

select country,role,count(*) as player_count from players group by country, role having player_count >1 order by player_count desc;

use ltim;

select * from emp;

select deptno,job, sum(sal) as totsal, max(sal) as maxsal, avg(sal) as avgsal
from emp
group by deptno, job
order by deptno,job desc;

select deptno,job, sum(sal) as totsal, max(sal) as maxsal, avg(sal) as avgsal
from emp
where deptno=10
group by deptno, job
having avgsal>2000
order by deptno,job desc;

select ename,(sal+IFNULL(comm,0)) as gross_sal from emp;

select ename, coalesce(sal,0)+coalesce(comm,0) as gross_sal from emp;

select * from emp where not isnull(comm);

select ename, comm,
case
	when comm is null then 0
    else comm
end as comm_notNull
from emp;

select ename,sal, comm,
case 
	when comm is null then sal
    else comm+sal
end as gross_sal
from emp;