DataBase: MySQL (Server version: 8.0.26)

Q1.

1. If a employee is in same meeting and same floor as covid positive employee, he/she will receive 2 notifications one mandatory and one optional.

2. Taken all dates range in between 2021-10-02 and 2021-10-07.

3. For Symptom, row_id is primary key and a employee can report multiple symptoms and all self-reported symptom will lead to a covid test.

3. For Scan, scan_id is primary key and a employee can have multiple scans on different occassions.

4. For Test, test_id is primary key and a employee can have multiple tests.

5. For Cases, a case is always reported on successful resolution. If case is not resolved it will not be there in table.

6. For Healthstatus, row_id is primary key, it maintains the current healthstatus of all employees. If employee latest status shows sick it means he is sick currently, however some previous date record may indicate he is well.

## --------------------------------------------------------------

Q2.

For most-self-reported symptom, I am just showing its ID.

I performed a group by on symptom_id and taken a count of employee id. The symptom_id with highest count is most-self-reported symptom.

## --------------------------------------------------------------

Q3. 
In Healthstatus table, I am considering there could be multiple entries per employee. However its latest update will denote his/her current status.

For sickest-floor, I am taking floor which has highest number of sick employee at the moment. For each employee I am taking its status at the latest date. For those employee whose latest record says status as "sick", I am taking a join on Employee table to get the floor. After that that I performed a group by on floor_number and get the employee count. The floor_number with the highest count is sickest floor.

Also, if we consider one record per employee in Healthsstatus table, I have written a commented query in Q2.sql.

## --------------------------------------------------------------

Q4. 
I have declared two variables as START_DATE and END_DATE.

For each of the stats, I have written a seperate query.

## --------------------------------------------------------------

Q5.

1. 
As a statistian, I would like to know is there a person who had attended all meetings where there has a covid-19 case. Those person might have high chances of becoming covid-19 positive, must be quarantined and tested.

I have used DIVIDE relational operation, to find out those employee.

Dividend Table: Meeting ( employee_id and meeting_id )
Divisor Table: ( All meeting_ids that is attended by atleast one covid positive employee.)
								--------
2. 
I would, also like to know employees who attends all meetings. That employee might have chances of becoming super-spreader.

This also can be DIVIDE relational operation.

Dividend table: Meeting (employee_id and meeting_id)
Divisor Table: (All Distinct meeting_ids)
