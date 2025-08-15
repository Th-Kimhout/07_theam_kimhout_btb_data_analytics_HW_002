--                                          SQL Homework

--1. Create a function to dynamically import CSV files into SQL tables using the CSV file's headers as column names

CREATE OR REPLACE FUNCTION import_csv_files(path_name text)
    RETURNS void
AS
$$

DECLARE
file_name    text;
    is_directory bool;
    full_path    text;

BEGIN
    -- Check if the provided path is a directory.
    is_directory := (SELECT isdir FROM PG_STAT_FILE(path_name));

    IF is_directory THEN
        -- If it's a directory, loop through all .csv files.
        FOR file_name IN
SELECT fname
FROM PG_LS_DIR(path_name) AS fname
WHERE fname LIKE '%.csv'
    LOOP
                -- Construct the full path using a forward slash for cross-platform compatibility.
                full_path := path_name || '/' || file_name;
RAISE NOTICE 'Processing file: %', full_path;
                PERFORM process_to_table(full_path);
END LOOP;
ELSE
        -- If it's a single file, just process that file.
        RAISE NOTICE 'Processing file: %', path_name;
        PERFORM process_to_table(path_name);
END IF;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION process_to_table(file_path text)
    RETURNS void
    LANGUAGE plpgsql
AS
$$
DECLARE
header_line  text;
    header_array text[];
    column_name  text;
    table_name   text := LOWER(REGEXP_REPLACE(file_path, '^.*[/\\]([^/\\]+)\.csv$', '\1'));
    copy_sql     text;
BEGIN
    -- Ensure the table name is a valid identifier (e.g., replace spaces/dashes with underscores)
    table_name := REGEXP_REPLACE(table_name, '[^a-zA-Z0-9_]', '_', 'g');

    RAISE NOTICE 'Creating table: %', table_name;

    -- Drop the table if it already exists to ensure a fresh import.
EXECUTE FORMAT('DROP TABLE IF EXISTS %I', table_name);

-- Create an empty PERMANENT table.

EXECUTE FORMAT('CREATE TABLE %I ()', table_name);

-- Read the first line of the file to get the column headers.

header_line := SPLIT_PART(PG_READ_FILE(file_path, 0, 8192), E'\n', 1);

    -- Add columns to the table based on the headers.
    header_array := STRING_TO_ARRAY(header_line, ',');

    FOREACH column_name IN ARRAY header_array
        LOOP
            --  Clean Column Name
            column_name := LOWER(TRIM(BOTH E' \t\r\n"' FROM column_name));
            column_name := REGEXP_REPLACE(column_name, '[^a-zA-Z0-9_]', '_', 'g');
            IF column_name LIKE '%id%' THEN
                EXECUTE FORMAT(
                        'ALTER TABLE %I ADD COLUMN %I bigint UNIQUE',
                        table_name,
                        column_name
                        );
ELSE
                EXECUTE FORMAT(
                        'ALTER TABLE %I ADD COLUMN %I text',
                        table_name,
                        column_name
                        );
END IF;

END LOOP;

    copy_sql := FORMAT(
            'COPY %I FROM %L WITH (FORMAT CSV, HEADER TRUE)',
            table_name,
            file_path
                );

    RAISE NOTICE 'Executing: %', copy_sql;
EXECUTE copy_sql;

END;
$$;

SELECT import_csv_files('D:\HRD\advanced_course_data_analytics\Homework\theam_kimhout_homework_2\Data');

--2. Create function that performs UPSERT operations (insert new records, update existing ones based on a key field)

CREATE OR REPLACE FUNCTION upsert_func(
    target_table text,
    schema_name text,
    csv_file_path text,
    key text
)
    RETURNS void
AS
$$
DECLARE
header_line     text;
    header_array    text[];
    column_name     text;
    temp_table_name text := LOWER(REGEXP_REPLACE(csv_file_path, '^.*[/\\]([^/\\]+)\.csv$', '\1')) || '_temp';
    copy_sql        text;
    is_exist        bool;
    cols            text;
sql             text;
BEGIN
    -- Sanitize temp table name
    temp_table_name := REGEXP_REPLACE(temp_table_name, '[^a-zA-Z0-9_]', '_', 'g');

    -- Drop temp table if it exists
EXECUTE FORMAT('DROP TABLE IF EXISTS %I', temp_table_name);

-- Read first line for headers
header_line := SPLIT_PART(PG_READ_FILE(csv_file_path, 0, 8192), E'\n', 1);

    -- Create empty temp table
EXECUTE FORMAT('CREATE TEMP TABLE %I ()', temp_table_name);

header_array := STRING_TO_ARRAY(header_line, ',');

    -- Add columns dynamically
    FOREACH column_name IN ARRAY header_array
        LOOP
            column_name := LOWER(TRIM(BOTH E' \t\r\n"' FROM column_name));
            column_name := REGEXP_REPLACE(column_name, '[^a-zA-Z0-9_]', '_', 'g');
            IF column_name LIKE '%id%' THEN
                EXECUTE FORMAT(
                        'ALTER TABLE %I ADD COLUMN %I bigint UNIQUE',
                        temp_table_name,
                        column_name
                        );
ELSE
                EXECUTE FORMAT(
                        'ALTER TABLE %I ADD COLUMN %I text',
                        temp_table_name,
                        column_name
                        );
END IF;
END LOOP;

    -- Copy CSV into temp table
    copy_sql := FORMAT(
            'COPY %I FROM %L WITH (FORMAT CSV, HEADER TRUE)',
            temp_table_name,
            csv_file_path
                );
EXECUTE copy_sql;

-- Check if target table exists
SELECT EXISTS (SELECT 1
               FROM information_schema.tables
               WHERE table_schema = schema_name
                 AND table_name = target_table)
INTO is_exist;

-- If exists, UPSERT
IF is_exist THEN
SELECT STRING_AGG(FORMAT('%I = EXCLUDED.%I', c.column_name, c.column_name), ', ')
INTO cols
FROM information_schema.columns c
WHERE table_schema = schema_name
  AND table_name = target_table
  AND c.column_name <> key;

sql := FORMAT(
                'INSERT INTO %I.%I SELECT * FROM %I
                 ON CONFLICT (%I) DO UPDATE SET %s',
                schema_name,
                target_table,
                temp_table_name,
                key,
                cols
               );
EXECUTE sql;
END IF;

    -- Drop temp table
EXECUTE FORMAT('DROP TABLE IF EXISTS %I', temp_table_name);
END;
$$ LANGUAGE plpgsql;


SELECT upsert_func('customers', 'public',
                   'D:\HRD\advanced_course_data_analytics\Homework\theam_kimhout_homework_2\Data\test_customers - Sheet1.csv',
                   'customer_id');


--3. Create a trigger function that automatically updates an audit log table whenever records are modified in the customer tables

CREATE TABLE customer_audit_log
(
    log_id    bigserial PRIMARY KEY,
    operation text,
    old_data  jsonb,
    new_data  jsonb,
    date_time timestamp
);

CREATE OR REPLACE FUNCTION customer_audit_trigger()
    RETURNS TRIGGER AS
$$
BEGIN
INSERT INTO customer_audit_log(operation,
                               old_data,
                               new_data,
                               date_time)
VALUES (TG_OP, -- 'INSERT', 'UPDATE', or 'DELETE'
        CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN ROW_TO_JSON(OLD) END,
        CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN ROW_TO_JSON(NEW) END,
        NOW());

RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER trg_customer_audit
    AFTER INSERT OR UPDATE OR DELETE
                    ON customers
                        FOR EACH ROW
                        EXECUTE FUNCTION customer_audit_trigger();

INSERT INTO customers(customer_name,
                      gender,
                      segment,
                      country,
                      city,
                      state,
                      age)
VALUES ('Maria Sanchez', 'F', 'Consumer', 'United States', 'Miami', 'Florida', 34),
       ('James Peterson', 'M', 'Corporate', 'United States', 'Seattle', 'Washington', 46),
       ('Linda Chen', 'F', 'Home Office', 'United States', 'Austin', 'Texas', 27),
       ('Robert Thompson', 'M', 'Consumer', 'United States', 'Chicago', 'Illinois', 39),
       ('Emily Johnson', 'F', 'Corporate', 'United States', 'New York', 'New York', 50),
       ('Daniel Martinez', 'M', 'Consumer', 'United States', 'Houston', 'Texas', 31),
       ('Sophia Williams', 'F', 'Corporate', 'United States', 'Boston', 'Massachusetts', 44),
       ('Michael Brown', 'M', 'Home Office', 'United States', 'Denver', 'Colorado', 29),
       ('Olivia Davis', 'F', 'Consumer', 'United States', 'Phoenix', 'Arizona', 38),
       ('William Wilson', 'M', 'Corporate', 'United States', 'San Francisco', 'California', 53),
       ('Ava Taylor', 'F', 'Consumer', 'United States', 'Atlanta', 'Georgia', 26),
       ('Benjamin Moore', 'M', 'Home Office', 'United States', 'Minneapolis', 'Minnesota', 41),
       ('Charlotte Anderson', 'F', 'Consumer', 'United States', 'Orlando', 'Florida', 35),
       ('Alexander Thomas', 'M', 'Corporate', 'United States', 'Detroit', 'Michigan', 48),
       ('Mia Jackson', 'F', 'Home Office', 'United States', 'Nashville', 'Tennessee', 32);

DELETE
FROM customers
WHERE age = '53';

--4. Design and implement a normalized database schema (3NF) for a movie rental system using core tables such as :
--      1. customer (customer_id, customer_name, gender, age, phone, email)
--      2. movie (movie_id, title, year, rating, runtime, release_date)
--      3. genre (genre_id, genre_name)
--      4. director (director_id, director_name)

-- This is a snippet of data from movie table
-- movie_id	    title	                        year	rating	runtime	genre	                    released	director	            imdbrating	imdbvotes	plot	        fullplot	    language	country	awards	lastupdated	    type
-- 1	        Pauvre Pierrot	                1892	        4 min	Animation, Comedy, Short	1892-10-28	mile Reynaud	        6.7	        566	        One night...	One night...	            France		    06:02.7	        movie
-- 2	        Blacksmith Scene	            1893    UNRATED	1 min	Short	                    1893-05-09	William K.L. Dickson	6.2	        1189	    Three men...	A stationary...		        USA	    1 win.	03:50.1	        movie
-- 3	        Edison Kinetoscopic...          1894	        1 min	Documentary, Short	        1894-01-09	William K.L. Dickson	5.9	        988	        A man...		A man...                    USA		        21:07.1	        movie
-- 4	        Tables Turned on the Gardener	1895	        1 min	Comedy, Short		                    Louis Lumre	            7.1	        2554	    A gardener...	A gardener...		        France		    06:18.2	        movie
-- 13	        Jack and the Beanstalk	        1902		    10 min	Short, Fantasy	            7/15/1902	George S. Fleming,
--                                                                                                              Edwin S. Porter	        6.2	        442	        Porter's...	    Porter's...	    English	    USA		        25:25.4	        movie

-- This table is not in the 1st Normal Form yet.
-- Because some columns contain multiple values like genre and director.
-- Other columns are okay since they contain atomic value and value is in the same domain

-- After Doing 1st Normal Form, we got this table

--                                          1st Normal Form Table

-- movie_id	    title	                        year	rating	runtime	genre	         released	    director	            imdbrating	imdbvotes	plot	        fullplot	    language	country	awards	lastupdated	    type
-- 1	        Pauvre Pierrot	                1892	        4 min	Animation        1892-10-28	    mile Reynaud	        6.7	        566	        One night...	One night...	            France		    06:02.7	        movie
-- 1	        Pauvre Pierrot	                1892	        4 min	Comedy	         1892-10-28	    mile Reynaud	        6.7	        566	        One night...	One night...	            France		    06:02.7	        movie
-- 1	        Pauvre Pierrot	                1892	        4 min	Short	         1892-10-28	    mile Reynaud	        6.7	        566	        One night...	One night...	            France		    06:02.7	        movie
-- 2	        Blacksmith Scene	            1893    UNRATED	1 min	Short	         1893-05-09	    William K.L. Dickson	6.2	        1189	    Three men...	A stationary...		        USA	    1 win.	03:50.1	        movie
-- 3	        Edison Kinetoscopic...          1894	        1 min	Documentary      1894-01-09	    William K.L. Dickson	5.9	        988	        A man...		A man...                    USA		        21:07.1	        movie
-- 3	        Edison Kinetoscopic...          1894	        1 min	Short            1894-01-09	    William K.L. Dickson	5.9	        988	        A man...		A man...                    USA		        21:07.1	        movie
-- 4	        Tables Turned on the Gardener	1895	        1 min	Comedy      	                Louis Lumre	            7.1	        2554	    A gardener...	A gardener...		        France		    06:18.2	        movie
-- 4	        Tables Turned on the Gardener	1895	        1 min	Short       	                Louis Lumre	            7.1	        2554	    A gardener...	A gardener...		        France		    06:18.2	        movie
-- 13	        Jack and the Beanstalk	        1902	        10 min	Short	        1902-07-15	    George S. Fleming	    6.2	        442	        Porter's...	    Porter's...	    English	    USA		        25:25.4	        movie
-- 13	        Jack and the Beanstalk	        1902	        10 min	Short	        1902-07-15	    Edwin S. Porter	        6.2	        442	        Porter's...	    Porter's...	    English	    USA		        25:25.4	        movie
-- 13	        Jack and the Beanstalk	        1902	        10 min	Fantasy	        1902-07-15	    George S. Fleming	    6.2	        442	        Porter's...	    Porter's...	    English	    USA		        25:25.4	        movie
-- 13	        Jack and the Beanstalk	        1902	        10 min	Fantasy	        1902-07-15	    Edwin S. Porter	        6.2	        442	        Porter's...	    Porter's...	    English	    USA		        25:25.4	        movie

-- to make it into the 2nd Normal Form, we will remove the partial dependencies
-- Since to be able to identify one row of the record we need movie_id, genre and director
-- so those 3 will be our composite key and have table as below

--                                          2nd Normal Form Table

--                                                 genres
-- genre_id [PK]        genre_name
-- 1                Animation
-- 2                Comedy
-- 3                Short
-- 4                Documentary
-- 5                Fantasy

--                                                 directors
-- director_id [PK]        director_name
-- 1                   mile Reynaud
-- 2                   William K.L. Dickson
-- 3                   Louis Lumre
-- 4                   George S. Fleming
-- 5                   Edwin S. Porter

-- movies( [PK] movie_id, title, year, rating, runtime, released, imdbrating, imdbvotes, plot, fullplot, language, country, awards, lastupdated, type )

-- movie_genres ( [PK] (movie_id, genre_id) )
-- movie_directors ([PK] (movie_id, director_id) )

-- A total of 5 tables created with 2nd Normal Form

-- Since there is no transitive dependency, so the table is already in the 3rd Normal Form

--                                          3rd Normal Form Table

-- movies( [PK] movie_id, title, year, rating, runtime, released, imdbrating, imdbvotes, plot, fullplot, language, country, awards, lastupdated, type )
-- directors ( [PK] director_id, director_name )
-- genres( [PK] genre_id, genre_name )
-- movie_genres ( [PK] (movie_id, genre_id) )
-- movie_directors ( [PK] (movie_id, director_id) )

-- A total of 5 tables created with 3rd Normal Form

-- With the core table provided, and to finish the rental system, we will have the table as below
-- 1. customer ( [PK] customer_id, customer_name, gender, age, phone, email )
-- 2. movie ( [PK] movie_id, title, year, rating, runtime, release_date )
-- 3. movie_rental ( [PK] rental_id, customer_id, movie_id, rental_date, due_date, status )
-- 4. genre ( [PK] genre_id, genre_name )
-- 5. director ( [PK] director_id, director_name )
-- 6. movie_genre ( [PK] (movie_id, genre_id) )
-- 7. movie_director ( [PK] (movie_id, director_id) )


--- Table: customer
CREATE TABLE customer
(
    customer_id   SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    gender        VARCHAR(10),
    age           INTEGER,
    phone         VARCHAR(20),
    email         VARCHAR(100) UNIQUE
);

--- Table: movie

CREATE TABLE movie
(
    movie_id     SERIAL PRIMARY KEY,
    title        VARCHAR(255) NOT NULL,
    year         INTEGER,
    rating       text,
    runtime      text,
    release_date DATE
);

--- Table: genre
CREATE TABLE genre
(
    genre_id   SERIAL PRIMARY KEY,
    genre_name VARCHAR(50) UNIQUE NOT NULL
);

--- Table: director
CREATE TABLE director
(
    director_id   SERIAL PRIMARY KEY,
    director_name VARCHAR(100) NOT NULL
);

--- Table: movie_rental
CREATE TABLE movie_rental
(
    rental_id   SERIAL PRIMARY KEY,
    customer_id INT  NOT NULL REFERENCES customers (customer_id),
    movie_id    INT  NOT NULL REFERENCES movies (movie_id),
    rental_date DATE NOT NULL,
    due_date    DATE NOT NULL,
    return_date DATE,
    status      VARCHAR(20) DEFAULT 'rented' CHECK (status IN ('rented', 'returned', 'late', 'lost'))
);


--- Table: movie_genre
CREATE TABLE movie_genre
(
    movie_id INTEGER,
    genre_id INTEGER,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movie (movie_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genre (genre_id) ON DELETE CASCADE
);

--- Table: movie_director
CREATE TABLE movie_director
(
    movie_id    INTEGER,
    director_id INTEGER,
    PRIMARY KEY (movie_id, director_id),
    FOREIGN KEY (movie_id) REFERENCES movie (movie_id) ON DELETE CASCADE,
    FOREIGN KEY (director_id) REFERENCES director (director_id) ON DELETE CASCADE
);

--5. Write an SQL script that loads the CSV and populates all normalized tables except customer.

-- Load Movies

CREATE TEMP TABLE staging_movie
(
    LIKE movies INCLUDING ALL
);
COPY staging_movie
    FROM 'D:\HRD\advanced_course_data_analytics\Homework\theam_kimhout_homework_2\Data\movies.csv'
    DELIMITER ','
    CSV HEADER;

INSERT INTO movie
SELECT movie_id, title, year::int, rating::int, runtime, released::date
FROM staging_movie;


CREATE TEMP TABLE staging_movie
(
    LIKE movies INCLUDING ALL
);
COPY staging_movie
    FROM 'D:\HRD\advanced_course_data_analytics\Homework\theam_kimhout_homework_2\Data\movies.csv'
    DELIMITER ','
    CSV HEADER;

INSERT INTO movie
SELECT movie_id, title, year::int, rating, runtime, released::date
FROM staging_movie;

-- Load Genres
INSERT INTO genre (genre_name)
SELECT DISTINCT TRIM(UNNEST(STRING_TO_ARRAY(s.genre, ',')))
FROM staging_movie AS s
    ON CONFLICT (genre_name) DO NOTHING;

INSERT INTO movie_genre (movie_id, genre_id)
SELECT s.movie_id,
       g.genre_id
FROM staging_movie AS s,
     UNNEST(STRING_TO_ARRAY(s.genre, ',')) AS s_genre
         JOIN
     genre AS g ON TRIM(s_genre) = g.genre_name
    ON CONFLICT DO NOTHING;

--  Load Directors
INSERT INTO director (director_name)
SELECT DISTINCT TRIM(UNNEST(STRING_TO_ARRAY(s.director, ',')))
FROM staging_movie AS s;

INSERT INTO movie_director (movie_id, director_id)
SELECT s.movie_id,
       d.director_id
FROM staging_movie AS s,
     UNNEST(STRING_TO_ARRAY(s.director, ',')) AS s_director
         JOIN
     director AS d ON TRIM(s_director) = d.director_name
    ON CONFLICT DO NOTHING;

-- Demo Data in movie rental

INSERT INTO customer (customer_id, customer_name, gender, age, phone, email)
VALUES (1, 'John Smith', 'Male', 35, '555-1234', 'john.smith@email.com'),
       (2, 'Jane Doe', 'Female', 28, '555-5678', 'jane.doe@email.com'),
       (3, 'Peter Jones', 'Male', 42, '555-9012', 'peter.jones@email.com');

INSERT INTO movie_rental
(customer_id, movie_id, rental_date, due_date, return_date, status)
VALUES
    (1, 101, '2025-08-01', '2025-08-05', '2025-08-04', 'returned'),
    (2, 102, '2025-08-02', '2025-08-06', '2025-08-08', 'late'),
    (3, 103, '2025-08-10', '2025-08-14', NULL, 'rented'),
    (1, 101, '2025-08-12', '2025-08-16', NULL, 'rented');



--6. Create a comprehensive view that joins all tables to show: customer name, age, movie title, release date, director name, genre name, rental date
DROP VIEW rental_detail;
CREATE OR REPLACE VIEW rental_detail AS
WITH movie_genres AS (
    -- First, create a subquery or CTE to aggregate all genres for each movie into a single string.
    SELECT mg.movie_id,
           STRING_AGG(g.genre_name, ', ' ORDER BY g.genre_name) AS genres
    FROM movie_genre AS mg
             JOIN
         genre AS g ON mg.genre_id = g.genre_id
    GROUP BY mg.movie_id)
   , movie_directors AS (SELECT md.movie_id,
                                STRING_AGG(d.director_name, ', ' ORDER BY d.director_name) AS director
                         FROM movie_director md
                                  JOIN
                              director AS d ON md.director_id = d.director_id
                         GROUP BY md.movie_id)

SELECT c.customer_name,
       c.age,
       m.title,
       m.release_date,
       md.director AS director_name,
       mg.genres   AS genre_name,
       mr.rental_date
FROM customer AS c
         INNER JOIN
     movie_rental AS mr ON c.customer_id = mr.customer_id
         INNER JOIN
     movie AS m ON mr.movie_id = m.movie_id
         INNER JOIN
     movie_directors AS md ON m.movie_id = md.movie_id
         INNER JOIN
     movie_genres AS mg ON m.movie_id = mg.movie_id;

SELECT *
FROM rental_detail;

--7. Write an SQL script to export query results to CSV format

COPY ( SELECT *
       FROM rental_detail) TO 'D:\HRD\advanced_course_data_analytics\Homework\theam_kimhout_homework_2\Data\rental_detail.csv' WITH (
           FORMAT CSV,
           HEADER TRUE
           );