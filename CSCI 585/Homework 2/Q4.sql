-- Q4 The management would like stats, for a given period (between start, end dates), on the following: 
-- number of scans, number of tests, number of employees who self-reported symptoms, number of positive cases. Write queries to output these.

-- Number of scans: 
SELECT COUNT(*) 
FROM Scan 
WHERE scan_date BETWEEN "2021-10-03" AND "2021-10-05";


-- Number of Tests
SELECT COUNT(*) 
FROM Test 
WHERE test_date BETWEEN "2021-10-03" AND "2021-10-05";


-- Number of Employees who self-reported sysmptoms
SELECT COUNT(DISTINCT employee_id) AS count 
FROM Symptom;


-- Number of positive cases
SELECT COUNT(*) 
FROM Test 
WHERE test_result = "positive";