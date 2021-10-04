			COVID CONTACT TRACING SYSTEM

Design choices I made while formulating my ER Diagram.

1. Building, Floor, Room
There can be multiple FLOORs in a BUILDING and each floor has its own unique Id. Also, each floor has multiple ROOM each with its own unique id. Rooms are classified as meeting rooms and office rooms.

2. App, Employee
Each EMPLOYEE uses an APP whose details is unique to individual employee. One employee cannot use two apps and one app cant be used by 2 employees. This way we can make sure that everybody gets notification or alerts on time.

3. Daily Status, Test Report, Self Report
During facility checkin screening of employees is being conducted this is stored in DAILY STATUS table. If a employee self report it will appear an SELF REPORT entity. After self report it follows a Covid test of that employee. Also it during screening, if employee have have high temperature or employee is selected for random testing then it follows a Covid test. All covid test related information in TEST_REPORT.

4. Meeting, Meeting Participants
Each MEETING is conducted in a Meeting Room. Each meeting room is associated with an MEETING_ROOM_ID. Each Meeting can be identified using MEETING_ID, and also meeting has attendes which can stored in MEETING PARTICIPANTS entity.

5. Alert Notification
For Contact Tracing, if an employee tests positive, all employee in its floor are notified, FloorID is attribute of an employee table itself. Also we can find which meetings that covid-positive employee has attended using “MEETING PARTICIPANTS” entity. And later on all employees who had attended these meetings will be notified through application notification alert or email if email is provided.

6. Health Status
Quarantined or hospitalized employees will update their health status which can be found out via HEALTH STATUS entity. Last status update can be found out using datetime attribute. Status can be “sick”, “hospitalized”, “well” or “deceased”. Also, employee is quarantined then quarantine location can be put in Health Status Entity.

