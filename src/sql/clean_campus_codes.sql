-- PSU Crime Analysis — Campus Cleanup SQL
-- Run against psu_crime_log.db (SQLite)

-- View incidents per campus
SELECT c.campus_name, COUNT(i.id) AS incident_count
FROM incidents i
JOIN campuses c ON i.campus_id = c.campus_id
GROUP BY c.campus_name
ORDER BY incident_count DESC;

-- All incidents with campus name
SELECT i.id, i.incident_number, c.campus_name,
       i.reported_datetime, i.nature_of_incident, i.location
FROM incidents i
JOIN campuses c ON i.campus_id = c.campus_id
ORDER BY i.reported_datetime DESC
LIMIT 50;

-- Incident frequency by hour
SELECT
    CAST(strftime('%H', substr(i.reported_datetime, 7, 4)||'-'||
         substr(i.reported_datetime, 1, 2)||'-'||
         substr(i.reported_datetime, 4, 2)||' '||
         substr(i.reported_datetime, 12)) AS INTEGER) AS hour_of_day,
    COUNT(*) AS incidents
FROM incidents i
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- Top 15 incident types
SELECT nature_of_incident, COUNT(*) AS cnt
FROM incidents
GROUP BY nature_of_incident
ORDER BY cnt DESC
LIMIT 15;

-- Top locations by campus
SELECT c.campus_name, i.location, COUNT(*) AS cnt
FROM incidents i JOIN campuses c ON i.campus_id=c.campus_id
GROUP BY c.campus_name, i.location
ORDER BY c.campus_name, cnt DESC;
