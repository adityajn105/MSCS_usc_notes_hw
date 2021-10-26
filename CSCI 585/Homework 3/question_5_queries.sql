CREATE EXTENSION POSTGIS;

CREATE TABLE MAP (loc_name VARCHAR, loc GEOMETRY);
INSERT INTO MAP VALUES
('Parkside','POINT(-118.290940 34.019003)'),
('Viterbi', 'POINT(-118.289289 34.020408)'),
('Bookstore', 'POINT(-118.286564 34.020883)'),
('TommyTrojan', 'POINT(-118.285369 34.020575)'),
('Marshal School', 'POINT(-118.283028 34.019122)'),
('Leavy Lib', 'POINT(-118.283053 34.021603)'),
('Dohenny Lib', 'POINT(-118.284095 34.020249)'),
('USC Religious', 'POINT(-118.284955 34.023143)'),
('Lyon Center', 'POINT(-118.287993 34.024106)'),
('Jefferson Ent', 'POINT(-118.283350 34.023199)'),
('USC Village', 'POINT(-118.285164 34.025190)'),
('Taper Hall', 'POINT(-118.284201 34.022172)'),
('Home', 'POINT(-118.287106 34.032605)');
	
SELECT loc_name, ST_ASText(loc) FROM MAP;

--Convex Hull
SELECT ST_ASTEXT(ST_CONVEXHULL(ST_COLLECT(loc))) FROM MAP;

--Nearest Neighbors of Home
SELECT loc_name, ST_ASTEXT(loc) as loc, ST_DISTANCE(loc,'POINT(-118.287106 34.032605)') as distance 
FROM MAP
WHERE loc_name <> 'Home'
ORDER BY ST_DISTANCE(loc,'POINT(-118.287106 34.032605)') 
limit 4;
