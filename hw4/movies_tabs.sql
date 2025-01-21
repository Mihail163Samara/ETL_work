CREATE TABLE movies (
    movie_id INT AUTO_INCREMENT PRIMARY KEY,
    movies_type VARCHAR(50),
    director VARCHAR(100),
    year_of_issue YEAR,
    length_in_minutes INT,
    rate DECIMAL(3, 1)
);
CREATE TABLE movies_before_1990 AS SELECT * FROM movies WHERE 1=0;
CREATE TABLE movies_1990_2000 AS SELECT * FROM movies WHERE 1=0;
CREATE TABLE movies_2000_2010 AS SELECT * FROM movies WHERE 1=0;
CREATE TABLE movies_2010_2020 AS SELECT * FROM movies WHERE 1=0;
CREATE TABLE movies_after_2020 AS SELECT * FROM movies WHERE 1=0;
CREATE TABLE movies_below_40 AS SELECT * FROM movies WHERE 1=0;
CREATE TABLE movies_40_90 AS SELECT * FROM movies WHERE 1=0;
CREATE TABLE movies_90_130 AS SELECT * FROM movies WHERE 1=0;
CREATE TABLE movies_above_130 AS SELECT * FROM movies WHERE 1=0;
CREATE TABLE movies_rate_below_5 AS SELECT * FROM movies WHERE 1=0;
CREATE TABLE movies_rate_5_8 AS SELECT * FROM movies WHERE 1=0;
CREATE TABLE movies_rate_8_10 AS SELECT * FROM movies WHERE 1=0;
DELIMITER $$

CREATE TRIGGER before_insert_movies
BEFORE INSERT ON movies
FOR EACH ROW
BEGIN
    IF NEW.year_of_issue < 1990 THEN
        INSERT INTO movies_before_1990 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSEIF NEW.year_of_issue BETWEEN 1990 AND 2000 THEN
        INSERT INTO movies_1990_2000 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSEIF NEW.year_of_issue BETWEEN 2000 AND 2010 THEN
        INSERT INTO movies_2000_2010 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSEIF NEW.year_of_issue BETWEEN 2010 AND 2020 THEN
        INSERT INTO movies_2010_2020 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSE
        INSERT INTO movies_after_2020 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    END IF;

    IF NEW.length_in_minutes < 40 THEN
        INSERT INTO movies_below_40 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSEIF NEW.length_in_minutes BETWEEN 40 AND 90 THEN
        INSERT INTO movies_40_90 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSEIF NEW.length_in_minutes BETWEEN 90 AND 130 THEN
        INSERT INTO movies_90_130 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSE
        INSERT INTO movies_above_130 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    END IF;

    IF NEW.rate < 5 THEN
        INSERT INTO movies_rate_below_5 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSEIF NEW.rate BETWEEN 5 AND 8 THEN
        INSERT INTO movies_rate_5_8 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSEIF NEW.rate BETWEEN 8 AND 10 THEN
        INSERT INTO movies_rate_8_10 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    END IF;
END;
$$

DELIMITER ;
INSERT INTO movies (movies_type, director, year_of_issue, length_in_minutes, rate) 
VALUES 
('Drama', 'Director A', 1985, 120, 7.8),
('Comedy', 'Director B', 1995, 85, 6.4),
('Action', 'Director C', 2005, 140, 8.5),
('Horror', 'Director D', 2015, 75, 4.3),
('Sci-Fi', 'Director E', 2023, 150, 9.0),
('Fantasy', 'Director F', 2022, 95, 8.2);
INSERT INTO movies (movies_type, director, year_of_issue, length_in_minutes, rate) 
VALUES 
('Experimental', 'Director G', 2021, 120, 10.5),
('Art House', 'Director H', 2023, 110, 11.0);
SELECT * FROM movies
UNION ALL
SELECT * FROM movies_before_1990
UNION ALL
SELECT * FROM movies_1990_2000
UNION ALL
SELECT * FROM movies_2000_2010
UNION ALL
SELECT * FROM movies_2010_2020
UNION ALL
SELECT * FROM movies_after_2020;
SELECT * FROM movies;
