-- DataBase USED: MySQL (Server version: 8.0.26)


-- FOR EACH symptom_id count how many type it appears in table then sort in descending order and Display first one.
SELECT symptom_id 
FROM ( 
	SELECT symptom_id, count(symptom_id) AS count 
	FROM Symptom 
	GROUP BY symptom_id 
	ORDER BY count DESC 
	) t1 
LIMIT 1;
