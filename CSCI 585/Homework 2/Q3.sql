-- Q3 (1 point). Write a query to output the 'sickest' floor.

-- Get the record of latest date

-- Assumption,
-- * Latest update in Healthstatus is always reported.
-- * To find out if employee is sick or not we will consider his/her last update. (means sickest floor at current time stamp)


-- After getting each employee and its latest record. Filter by if they are sick, find their floor_number by doing join on employee id, 
-- Then group by floor_number to count how many sick employees on each floor, 
-- Sort by number of sick employees in descending order, then display the floor_number on first row..

SELECT floor_number 
FROM (
	SELECT COUNT(t1.employee_id) AS sick_count, e.floor_number 
	FROM Healthstatus t1 
	INNER JOIN ( 
		-- select employee_id and date with latest date for each employee
		SELECT employee_id, MAX(date) AS date 
		FROM Healthstatus 
		GROUP BY employee_id 
		) t2 
	ON t1.employee_id = t2.employee_id AND t1.date = t2.date 
	INNER JOIN Employee e 
	ON t1.employee_id = e.id 
	WHERE t1.status='sick' 
	GROUP BY e.floor_number 
	ORDER BY sick_count DESC 
	) t1 
LIMIT 1;


--- If we assume there is only one row per employee in Healthstatus table, and particular entry show latest update, then query become quite simple.

-- SELECT floor_number 
-- FROM ( 
-- 	SELECT COUNT(e.id) AS sick_count, e.floor_number 
-- 	FROM Healthstatus h INNER JOIN Employee e 
-- 	ON h.employee_id=e.id 
-- 	WHERE h.status='sick' 
-- 	GROUP BY e.floor_number 
-- 	ORDER BY sick_count DESC 
-- 	) t1 
-- LIMIT 1;
