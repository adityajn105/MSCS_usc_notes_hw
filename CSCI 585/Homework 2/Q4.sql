-- DataBase USED: MySQL (Server version: 8.0.26)


SET @START_DATE = '2021-10-03';
SET @END_DATE = '2021-10-05';

-- Number of scans: 
SELECT COUNT(*) AS number_of_scans 
FROM Scan 
WHERE scan_date BETWEEN @START_DATE AND @END_DATE;


-- Number of Tests
SELECT COUNT(*) AS number_of_tests
FROM Test 
WHERE test_date BETWEEN @START_DATE AND @END_DATE;


-- Number of Employees who self-reported sysmptoms
SELECT COUNT(DISTINCT employee_id) AS number_of_self_reports 
FROM Symptom
WHERE date_reported BETWEEN @START_DATE AND @END_DATE;



-- Number of positive cases
SELECT COUNT(*) AS number_of_positive_cases
FROM Test 
WHERE test_result = "positive" AND test_date BETWEEN @START_DATE AND @END_DATE;