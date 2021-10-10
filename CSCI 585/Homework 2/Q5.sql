-- DataBase USED: MySQL (Server version: 8.0.26)


-- employees who attended all meetings attended by atleast one covid 19 positive employee, 
-- have high chances of becoming covid-19 positive later on. Should be quarantined for one week

-- Here I have implemented division between meeting_id, employee projection of Meeting table and meeting_id attended by covid positive employees.

SELECT employee_id 
FROM Meeting 
WHERE meeting_id IN ( 
	SELECT DISTINCT(meeting_id) 
	FROM Meeting 
	WHERE employee_id IN (
		SELECT employee_id 
		FROM Test
		WHERE test_result='positive'
		) 
	)
GROUP BY employee_id 
HAVING COUNT(meeting_id) = ( 
	SELECT COUNT(DISTINCT(meeting_id)) 
	FROM Meeting 
	WHERE employee_id IN (
		SELECT employee_id 
		FROM Test
		WHERE test_result='positive'
		)  
	);



-- employee who attended all meeting have chances of becoming super spreader, and must be checked
SELECT employee_id 
FROM Meeting 
GROUP BY employee_id 
HAVING COUNT(meeting_id) = (
	SELECT COUNT(DISTINCT(meeting_id)) 
	FROM Meeting
	);