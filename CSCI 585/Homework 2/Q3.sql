-- DataBase USED: MySQL (Server version: 8.0.26)

SELECT floor_number AS sickest_floor
FROM (
	SELECT COUNT(t1.employee_id) AS sick_count, e.floor_number 
	FROM Healthstatus t1 
	INNER JOIN ( 
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


-- If we assume there is only one row per employee in Healthstatus table, and particular entry show latest update, then query become quite simple.

-- SELECT floor_number AS sickest_floor
-- FROM ( 
-- 	SELECT COUNT(e.id) AS sick_count, e.floor_number 
-- 	FROM Healthstatus h INNER JOIN Employee e 
-- 	ON h.employee_id=e.id 
-- 	WHERE h.status='sick' 
-- 	GROUP BY e.floor_number 
-- 	ORDER BY sick_count DESC 
-- 	) t1 
-- LIMIT 1;