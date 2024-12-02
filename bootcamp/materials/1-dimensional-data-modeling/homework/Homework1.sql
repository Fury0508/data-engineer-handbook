-- CREATE TYPE films as (
--     film TEXT,
--     votes INTEGER,
--     rating REAL,
--     filmid TEXT
-- )

-- CREATE TYPE quality_class AS ENUM('star','good','average','bad');

-- CREATE TABLE actors(
--     actor TEXT,
--     actorid TEXT,
--     year INTEGER,
--     films films[],
--     quality_class quality_class,
--     is_actor BOOLEAN NOT NULL DEFAULT TRUE 

-- )

-- DROP TABLE actors;
-- SELECT * from players;

INSERT INTO actors
-- WITH cte1 as (
--     SELECT * FROM actor_films
--     where year = 1997
-- ),
-- cte2 AS(
--     SELECT * FROM actors where 
--     year =  1997
    
-- )
-- SELECT 
--     COALESCE(c2.actor, c1.actor) AS actor,
--     COALESCE(c2.actorid,c1.actorid) AS actorid,
--     COALESCE(c2.year, c1.year) AS year,
--     CASE 
--         WHEN c2.films IS NULL THEN ARRAY[ROW(c1.film,c1.votes,c1.rating,c1.filmid)::films]
--         WHEN c1.film IS NOT NULL THEN c2.films || ARRAY[ROW(c1.film,c1.votes,c1.rating,c1.filmid)::films]
--         ELSE c2.films
--     END AS films,
--     CASE 
--         WHEN c1.year IS NOT NULL THEN
--         CASE WHEN c1.rating > 8 THEN 'star'
--         WHEN c1.rating >7 and c1.rating <= 8  THEN 'good'
--         WHEN c1.rating >6 and c1.rating <= 7  THEN 'average'
--         ELSE 'bad'
--     END :: quality_class
--     ELSE c2.quality_class end as quality_class,
--     CASE 
--         WHEN EXTRACT(YEAR FROM CURRENT_DATE) = GREATEST(c2.year, c1.year) THEN TRUE
--         ELSE FALSE
--     END AS is_actor
-- FROM cte1 c1 FULL OUTER JOIN cte2 c2 
-- on c1.actor = c2.actor


-- WITH cte1 AS (
--     SELECT *, ARRAY_AGG(ROW(film, votes, rating, filmid)::films) OVER (PARTITION BY actor, year) AS films_grouped
--     FROM actor_films
--     WHERE year = 1997
-- ),
-- cte2 AS (
--     SELECT * FROM actors
--     WHERE year = 1997
-- )
-- SELECT
--     COALESCE(c2.actor, c1.actor) AS actor,
--     COALESCE(c2.actorid, c1.actorid) AS actorid,
--     COALESCE(c2.year, c1.year) AS year,
--     c1.films_grouped AS films,
--     CASE 
--         WHEN c1.year IS NOT NULL THEN
--             CASE WHEN c1.rating > 8 THEN 'star'
--                  WHEN c1.rating > 7 AND c1.rating <= 8 THEN 'good'
--                  WHEN c1.rating > 6 AND c1.rating <= 7 THEN 'average'
--                  ELSE 'bad'
--             END :: quality_class
--         ELSE c2.quality_class
--     END AS quality_class,
--     CASE 
--         WHEN EXTRACT(YEAR FROM CURRENT_DATE) = GREATEST(c2.year, c1.year) THEN TRUE
--         ELSE FALSE
--     END AS is_actor
-- FROM cte1 c1
-- FULL OUTER JOIN cte2 c2
-- ON c1.actor = c2.actor;


WITH cte1 AS (
    SELECT *, ARRAY_AGG(ROW(film, votes, rating, filmid)::films) OVER (PARTITION BY actor, year) AS films_grouped
    FROM actor_films
    WHERE year = 1997
),
cte2 AS (
    SELECT * FROM actors
    WHERE year = 1997
)
SELECT
    COALESCE(c2.actor, c1.actor) AS actor,
    COALESCE(c2.actorid, c1.actorid) AS actorid,
    COALESCE(c2.year, c1.year) AS year,
    COALESCE(c1.films_grouped, ARRAY[]::films[]) AS films,  -- Replace NULL with an empty array
    CASE 
        WHEN c1.year IS NOT NULL THEN
            CASE WHEN c1.rating > 8 THEN 'star'
                 WHEN c1.rating > 7 AND c1.rating <= 8 THEN 'good'
                 WHEN c1.rating > 6 AND c1.rating <= 7 THEN 'average'
                 ELSE 'bad'
            END :: quality_class
        ELSE c2.quality_class
    END AS quality_class,
    CASE 
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) = GREATEST(c2.year, c1.year) THEN TRUE
        ELSE FALSE
    END AS is_actor
FROM cte1 c1
FULL OUTER JOIN cte2 c2
ON c1.actor = c2.actor
WHERE c1.actor IS NOT NULL OR c2.actor IS NOT NULL;   -- Filter based on actor existence



WITH RECURSIVE years_to_process AS (
  SELECT MIN(year) AS year
  FROM actor_films
  UNION ALL
  SELECT year + 1
  FROM years_to_process
  WHERE year < (SELECT MAX(year) FROM actor_films)
)
INSERT INTO actors (actor, actorid, year, films, quality_class, is_actor)
SELECT
  af.actor,
  af.actorid,
  af.year,
  ARRAY_AGG(ROW(af.film, af.votes, af.rating, af.filmid)::films) AS films,
  CASE
    WHEN AVG(af.rating) > 8 THEN 'star'::quality_class
    WHEN AVG(af.rating) > 7 THEN 'good'::quality_class
    WHEN AVG(af.rating) > 6 THEN 'average'::quality_class
    ELSE 'bad'::quality_class
  END AS quality_class,
  TRUE AS is_actor
FROM actor_films af
JOIN years_to_process ytp ON af.year = ytp.year
GROUP BY af.actor, af.actorid, af.year
ORDER BY af.year;

SELECT * FROM actors where actor = 'Aamir Khan';