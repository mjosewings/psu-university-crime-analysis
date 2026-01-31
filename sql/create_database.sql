-- Database for Penn State Campuses
CREATE DATABASE psu_campuses;

-- Campuses Table
CREATE TABLE campuses(
    campus_id INT AUTO_INCREMENT PRIMARY KEY,
    campus_name VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(2) NOT NULL
);

-- Locations Table 
CREATE TABLE locations(
    location_id INT AUTO_INCREMENT PRIMARY KEY,
    campus_id INT NOT NULL,
    location_name VARCHAR(100) NOT NULL,
    FOREIGN KEY (campus_id) REFERENCES campuses(campus_id)
);

-- Incidents Table
CREATE TABLE incidents(
    incident_id INT PRIMARY KEY,
    campus_id INT NOT NULL,
    location_id INT NOT NULL,
    nature_of_incident VARCHAR(255),
    reported_datetime DATETIME,
    occurred_datetime DATETIME,
    FOREIGN KEY (campus_id) REFERENCES campuses(campus_id),
    FOREIGN KEY (location_id) REFERENCES locations(location_id)
);


-- Offenses Table
CREATE TABLE offenses(
    offense_id INT PRIMARY KEY,
    incident_id INT NOT NULL,
    offense_description VARCHAR(255),
    FOREIGN KEY (incident_id) REFERENCES incidents(incident_id)
);

-- Nature of Incidents Table
CREATE TABLE nature_of_incidents(
    nature_id INT AUTO_INCREMENT PRIMARY KEY,
    nature_name VARCHAR(255) UNIQUE
);
