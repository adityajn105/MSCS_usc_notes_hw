-- DataBase USED: MySQL (Server version: 8.0.26)

-- Create and use DataBase
-- CREATE DATABASE db_hw2;
-- USE db_hw2;


CREATE TABLE Employee (  
	id INT(5) UNIQUE NOT NULL,  
	name VARCHAR(40) NOT NULL,  
	ofc_number VARCHAR(3),  
	floor_number INT(2) NOT NULL,  
	phone_number INT(10) NOT NULL,  
	email_address VARCHAR(20),  
	CONSTRAINT floor_number_ck1 CHECK (floor_number BETWEEN 1 AND 10),  PRIMARY KEY (id) 
);

INSERT INTO Employee ( id, name, ofc_number, floor_number, phone_number, email_address ) 
VALUES
( 12345, "John Doe", "A12", 10, 1234567890, "john.doe@xyz.com" ),
( 12346, "Joe Biden", "B32", 01, 1236547890, "biden.joe@dem.com"),
( 32456, "Barak Obama", "A43", 01, 1234869261, "obama.barak@dem.com" ),
( 78954, "Hillary Clinton", "B65", 01, 1876345213, "clinton@dem.com" ),
( 34512, "Donald Trump", "C12", 04, 1357658762, "trump@rep.com" ),
( 98123, "Kamala Harris", "B33", 01, 1213213211, "harris.kam@dem.com"),
( 98124, "Mike Pence", "A45", 04, 1891239876, "pence.mike@rep.com"),
( 87654, "George W. Bush", "C15", 04, 1871231231, "bush@rep.com"),
( 78345, "Jimmy Carter", "E15", 05, 1899877890, "jimmy@dem.com"),
( 98672, "Bernie Sanders", "E65", 05, 1673451232, "bernie@dem.com"),
( 87345, "Alexandria Cortez", "E78", 05, 1657834221, "cortez@dem.com"),
( 87452, "Nancy Pelosi", "D23", 05, 1673458912, "nancy@dem.com" ),
( 45678, "Ben Carson", "D54", 06, 1452178934, "ben@rep.com" ),
( 78321, "Arnold Schwarz", "E23", 06, 1532783242, "arnold@rep.com"),
( 67231, "Henry Kissinger", "E98", 06, 1654289123, "henry@rep.com"),
( 78213, "Ted Cruz", "E90", 06, 1789234451, "ted.cruz@rep.com"),
( 87634, "Sarah Palin", "F16", 06, 1782347863, "sarah@rep.com"),
( 56538, "Bob Dole", "F14", 06, 1435897543, "bob.dole@rep.com"),
( 78654, "Tim Scott", "F12", 06, 1679765412, "tim.scott@rep.com"),
( 56789, "Nikki Haley", "B02", 06, 1562340987, "nancy.haley@rep.com" ),
( 92345, "John Kerry", "B14", 03, 1456798765, "john.jerry@dem.com" ),
( 76543, "Andrew Yang", "C12", 03, 1789234654, "andrew.yang@dem.com" ),
( 45673, "Cory Booker", "C65", 03, 1672892145, "cory.booker@dem.com" ),
( 19812, "Maxine Waters", "C76", 03, 1458912333, "maxine@dem.com"),
( 78432, "John Lewis", "D43", 03, 1892348762, "john.lewis@dem.com"),
( 67540, "Al Gore", "D65", 03, 1673245789, "al.gore@dem.com" );



-- Meeting [contains meeting info, on every meeting between employees]: meeting ID, employee ID, room number, floor number, meeting start time (just an int between 8 and 18, standing for 8AM..6PM)
CREATE TABLE Meeting (
	meeting_id INT(5) NOT NULL,
	employee_id INT(5) NOT NULL,
	room_number INT(3) NOT NULL,
	floor_number INT(2) NOT NULL,
	meeting_start INT(2) NOT NULL,
	CONSTRAINT floor_number_ck2 CHECK (floor_number BETWEEN 1 AND 10),
	CONSTRAINT meeting_start_ck CHECK (meeting_start BETWEEN 8 AND 18),
	PRIMARY KEY (meeting_id, employee_id),
	FOREIGN KEY (employee_id) REFERENCES Employee(id)
);

INSERT INTO Meeting ( meeting_id, employee_id, room_number, floor_number, meeting_start )
VALUES
( 23568, 12346, 103, 01, 09 ),
( 23568, 12345, 103, 01, 09 ),
( 23568, 32456, 103, 01, 09 ),
( 23568, 45673, 103, 01, 09 ),
( 23569, 76543, 203, 01, 09 ),
( 23569, 78432, 203, 02, 09 ),
( 23569, 87654, 203, 02, 09 ),
( 16589, 98123, 305, 03, 08 ),
( 16589, 78432, 305, 03, 08 ),
( 16589, 12345, 305, 03, 09 ),
( 16589, 87345, 305, 03, 08 ),
( 16570, 45678, 305, 05, 08 ),
( 16570, 12345, 305, 05, 08 ),
( 16570, 78213, 305, 05, 08 ),
( 73519, 12345, 505, 05, 15 ),
( 73519, 87452, 505, 05, 15 ),
( 73520, 67540, 405, 05, 16 ),
( 73520, 12345, 405, 05, 16 ),
( 73520, 45673, 405, 05, 16 ),
( 73520, 56538, 405, 05, 16 ),
( 73520, 67231, 405, 05, 16 ),
( 32158, 78654, 305, 03, 11 ),
( 32158, 45678, 305, 03, 11 ),
( 32158, 87634, 305, 03, 11 ),
( 37890, 56538, 905, 09, 14 ),
( 37890, 56789, 905, 09, 14 ),
( 37890, 12345, 905, 09, 14 ),
( 37890, 67231, 905, 09, 14 ),
( 37890, 78213, 905, 09, 14 );



-- Notification [based on contact tracing, to alert employees who might have been exposed]: notification ID, employee ID, notification date, notification type (mandatory, optional)
CREATE TABLE Notification (
	notification_id INT NOT NULL,
	employee_id INT(5) NOT NULL,
	notification_date DATE,
	notification_type VARCHAR(9) NOT NULL,
	CONSTRAINT notification_type_ck CHECK (notification_type IN ('mandatory', 'optional') ),
	PRIMARY KEY (notification_id),
	FOREIGN KEY (employee_id) REFERENCES Employee(id)
);

INSERT INTO Notification ( notification_id, employee_id, notification_date, notification_type )
VALUES
( 1, 78954, '2021-10-03', 'optional'),
( 2, 32456, '2021-10-03', 'optional'),
( 3, 12346, '2021-10-03', 'optional'),
( 4, 98123, '2021-10-03', 'optional'),
( 5, 98124, '2021-10-02', 'optional'),
( 6, 34512, '2021-10-02', 'optional'),
( 7, 87654, '2021-10-02', 'optional'),
( 8, 45673, '2021-10-03', 'optional'),
( 9, 19812, '2021-10-03', 'optional'),
( 10, 67540, '2021-10-03', 'optional'),
( 11, 76543, '2021-10-03', 'optional'),
( 12, 78432, '2021-10-03', 'optional'),
( 13, 92345, '2021-10-03', 'optional'),
( 14, 12345, '2021-10-03', 'mandatory'),
( 15, 32456, '2021-10-03', 'mandatory'),
( 16, 45673, '2021-10-03', 'mandatory'),
( 17, 45673, '2021-10-03', 'mandatory'),
( 18, 56538, '2021-10-03', 'mandatory'),
( 19, 67231, '2021-10-03', 'mandatory'),
( 20, 67540, '2021-10-03', 'mandatory'),
( 21, 87634, '2021-10-04', 'optional'),
( 22, 45678, '2021-10-04', 'optional'),
( 23, 56538, '2021-10-04', 'optional'),
( 24, 56789, '2021-10-04', 'optional'),
( 25, 67231, '2021-10-04', 'optional'),
( 26, 78213, '2021-10-04', 'optional'),
( 27, 78321, '2021-10-04', 'optional'),
( 28, 78654, '2021-10-04', 'optional'),
( 29, 56538, '2021-10-04', 'mandatory'),
( 30, 56789, '2021-10-04', 'mandatory'),
( 31, 67231, '2021-10-04', 'mandatory'),
( 32, 78213, '2021-10-04', 'mandatory'),
( 33, 45673, '2021-10-04', 'mandatory'),
( 34, 56538, '2021-10-04', 'mandatory'),
( 35, 67231, '2021-10-04', 'mandatory'),
( 36, 67540, '2021-10-04', 'mandatory'),
( 37, 45673, '2021-10-04', 'optional'),
( 38, 19812, '2021-10-04', 'optional'),
( 39, 67540, '2021-10-04', 'optional'),
( 40, 76543, '2021-10-04', 'optional'),
( 41, 78432, '2021-10-04', 'optional'),
( 42, 92345, '2021-10-04', 'optional'),
( 43, 78432, '2021-10-04', 'mandatory'),
( 44, 87345, '2021-10-04', 'mandatory'),
( 45, 98123, '2021-10-04', 'mandatory'),
( 46, 45673, '2021-10-04', 'optional'),
( 47, 19812, '2021-10-04', 'optional'),
( 48, 67540, '2021-10-04', 'optional'),
( 49, 76543, '2021-10-04', 'optional'),
( 50, 78432, '2021-10-04', 'optional'),
( 51, 92345, '2021-10-04', 'optional'),
( 52, 34512, '2021-10-06', 'optional'),
( 53, 87654, '2021-10-06', 'optional'),
( 54, 98124, '2021-10-06', 'optional'),
( 55, 78345, '2021-10-06', 'optional'),
( 56, 87345, '2021-10-06', 'optional'),
( 57, 87452, '2021-10-06', 'optional'),
( 58, 98672, '2021-10-06', 'optional'),
( 59, 87634, '2021-10-04', 'optional'),
( 60, 45678, '2021-10-04', 'optional'),
( 61, 56538, '2021-10-04', 'optional'),
( 62, 56789, '2021-10-04', 'optional'),
( 63, 67231, '2021-10-04', 'optional'),
( 64, 78213, '2021-10-04', 'optional'),
( 65, 78321, '2021-10-04', 'optional'),
( 66, 78654, '2021-10-04', 'optional'),
( 67, 98124, '2021-10-07', 'optional'),
( 68, 78345, '2021-10-07', 'optional'),
( 69, 87345, '2021-10-07', 'optional'),
( 70, 87452, '2021-10-07', 'optional'),
( 71, 34512, '2021-10-02', 'optional'),
( 72, 87654, '2021-10-02', 'optional'),
( 73, 98124, '2021-10-02', 'optional');



-- Symptom [self-reported by employees, any of 5 symptoms]: row ID (1,2...), employee ID, date reported, symptom ID (1 through 5)
CREATE TABLE Symptom (
	row_id INT UNIQUE AUTO_INCREMENT,
	employee_id INT(5) NOT NULL,
	date_reported Date NOT NULL,
	symptom_id INT(1) NOT NULL,
	CONSTRAINT symptom_id_ck CHECK ( symptom_id BETWEEN 1 AND 5 ),
	PRIMARY KEY (row_id),
	FOREIGN KEY (employee_id) REFERENCES Employee(id)
);

INSERT INTO Symptom ( employee_id, date_reported, symptom_id )
VALUES
( 78954, "2021-10-02", 3 ),
( 87345, "2021-10-03", 5 ),
( 78345, "2021-10-02", 4 ),
( 12345, '2021-10-02', 2 ),
( 19812, '2021-10-03', 4 ),
( 34512, '2021-10-05', 5 ),
( 98672, '2021-10-05', 4 ),
( 92345, '2021-10-03', 1 );


-- Scan [random scans of employees' body temperatures]: scan ID, scan date, scan time, employee ID, temperature
CREATE TABLE Scan (
	scan_id INT UNIQUE NOT NULL,
	scan_date Date NOT NULL,
	scan_time INT(2) NOT NULL,
	employee_id INT(5) NOT NULL,
	temperature INT(2) NOT NULL,
	CONSTRAINT scan_time_ck CHECK (scan_time BETWEEN 0 AND 23),
	PRIMARY KEY (scan_id),
	FOREIGN KEY (employee_id) REFERENCES Employee(id)
);

INSERT INTO Scan ( scan_id, scan_date, scan_time, employee_id, temperature )
VALUES
( 72, '2021-10-02', 10, 98124, 100 ),
( 82, '2021-10-03', 11, 45673, 97 ),
( 83, '2021-10-04', 10, 78321, 101 ),
( 84, '2021-10-03', 11, 87654, 98 ),
( 85, '2021-10-04', 10, 78954, 96 ),
( 86, '2021-10-05', 08, 19812, 95 ),
( 87, '2021-10-03', 09, 67540, 95 ),
( 88, '2021-10-05', 11, 56538, 95 ),
( 90, '2021-10-06', 10, 78345, 98 ),
( 92, '2021-10-05', 09, 78954, 99 ),
( 93, '2021-10-02', 10, 98124, 100 );



-- Test [to record test details]: test ID, location (company or hospital or clinic etc), test date, test time, employee ID, test result (positive or negative)
CREATE TABLE Test (
	test_id INT UNIQUE NOT NULL,
	location VARCHAR(10) NOT NULL,
	test_date Date NOT NULL,
	test_time INT(2) NOT NULL,
	employee_id INT(5) NOT NULL,
	test_result VARCHAR(8) NOT NULL,
	CONSTRAINT test_time_ck CHECK (test_time BETWEEN 0 AND 23),
	CONSTRAINT test_result_ck CHECK (test_result IN ('positive', 'negative')),
	PRIMARY KEY (test_id),
	FOREIGN KEY (employee_id) REFERENCES Employee(id)
);

INSERT INTO Test ( test_id, location, test_date, test_time, employee_id, test_result )
VALUES
( 67, 'hospital', '2021-10-03', 12, 78954, 'positive' ),
( 70, 'company', '2021-10-02', 11, 98124, 'positive' ),
( 80, 'company', '2021-10-03', 12, 45673, 'positive' ),
( 81, 'company', '2021-10-04', 09, 12345, 'negative' ),
( 83, 'company', '2021-10-04', 10, 32456, 'negative' ),
( 85, 'company', '2021-10-04', 09, 56538, 'positive' ),
( 87, 'company', '2021-10-04', 11, 67231, 'negative' ),
( 90, 'company', '2021-10-04', 11, 67540, 'negative' ),
( 92, 'company', '2021-10-05', 10, 56789, 'negative' ),
( 93, 'company', '2021-10-05', 10, 67231, 'negative' ),
( 94, 'company', '2021-10-05', 09, 78213, 'negative' ),
( 95, 'company', '2021-10-05', 09, 45673, 'negative' ),
( 96, 'hospital', '2021-10-04', 11, 87345, 'positive' ),
( 102, 'company', '2021-10-06', 10, 98672, 'positive' ),
( 97, 'hospital', '2021-10-05', 11, 78432, 'negative' ),
( 98, 'hospital', '2021-10-05', 11, 98123, 'negative' ),
( 104, 'company', '2021-10-06', 11, 78345, 'positive' ),
( 99, 'hospital', '2021-10-05', 11, 78345, 'negative' ),
( 100, 'hospital', '2021-10-05', 11, 12345, 'negative' ),
( 101, 'company', '2021-10-06', 10, 34512, 'positive' ),
( 103, 'company', '2021-10-04', 10, 78321, 'positive' ),
( 109, 'company', '2021-10-05', 10, 78954, 'negative' ),
( 105, 'company', '2021-10-02', 11, 98124, 'positive' ),
( 106, 'company', '2021-10-03', 10, 92345, 'negative' );


-- Case [to record employees who test positive]: case ID, employee ID, date, resolution (back to work, left the company, or deceased)
CREATE TABLE Cases (
	case_id INT UNIQUE NOT NULL,
	employee_id INT(5) NOT NULL,
	date Date NOT NULL,
	resolution VARCHAR(16),
	CONSTRAINT resolution_ck CHECK (resolution IN ('back to work', 'left the company', 'deceased')),
	FOREIGN KEY (employee_id) REFERENCES Employee(id)
);

INSERT INTO Cases ( case_id, employee_id, date, resolution )
VALUES
( 25, 98124, '2021-10-06', 'back to work' ),
( 26, 78954, '2021-10-07', 'left the company' ),
( 27, 34512, '2021-10-07', 'left the company' ),
( 28, 78321, '2021-10-08', 'deceased' );


-- HealthStatus [self-reporting by employees]: row ID, employee ID, date, status (sick, hospitalized, well)
CREATE TABLE Healthstatus (
	row_id INT UNIQUE AUTO_INCREMENT,
	employee_id INT(5) NOT NULL,
	date Date NOT NULL,
	status VARCHAR(12),
	CONSTRAINT status_ck CHECK (status IN ('sick', 'hospitalized', 'well')),
	FOREIGN KEY (employee_id) REFERENCES Employee(id)
);

INSERT INTO Healthstatus ( employee_id, date, status)
VALUES
( 12345, '2021-10-01', 'well' ),
( 12346, '2021-10-01', 'well' ),
( 19812, '2021-10-01', 'well' ),
( 32456, '2021-10-01', 'well' ),
( 34512, '2021-10-01', 'well' ),
( 98124, '2021-10-01', 'well' ),
( 87654, '2021-10-01', 'well' ),
( 92345, '2021-10-01', 'well' ),
( 19812, '2021-10-01', 'well' ),
( 45673, '2021-10-01', 'well' ),
( 67540, '2021-10-01', 'well' ),
( 76543, '2021-10-01', 'well' ),
( 78432, '2021-10-01', 'well' ),
( 78345, '2021-10-01', 'well' ),
( 87345, '2021-10-01', 'well' ),
( 87452, '2021-10-01', 'well' ),
( 98672, '2021-10-01', 'well' ),
( 12345, '2021-10-01', 'well' ),
( 87634, '2021-10-01', 'well' ),
( 45678, '2021-10-01', 'well' ),
( 56538, '2021-10-01', 'well' ),
( 56789, '2021-10-01', 'well' ),
( 67231, '2021-10-01', 'well' ),
( 78213, '2021-10-01', 'well' ),
( 78654, '2021-10-01', 'well' ),
( 78954, '2021-10-02', 'hospitalized' ),
( 78954, '2021-10-03', 'sick' ),
( 98124, '2021-10-02', 'hospitalized' ),
( 98124, '2021-10-02', 'sick' ),
( 45673, '2021-10-03', 'sick' ),
( 56538, '2021-10-04', 'sick' ),
( 87345, '2021-10-04', 'sick' ),
( 34512, '2021-10-06', 'hospitalized' ),
( 34512, '2021-10-06', 'sick' ),
( 98672, '2021-10-06', 'sick' ),
( 78321, '2021-10-04', 'sick' ),
( 78345, '2021-10-06', 'sick' ),
( 98124, '2021-10-06', 'well' );
