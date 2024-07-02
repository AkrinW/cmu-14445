.timer on

WITH artist_decades AS (
    SELECT 
        LEFT(CAST(artist.begin_date_year AS CHAR), 3) AS first_three_digits,
        COUNT(*) AS group_count
    FROM artist
    JOIN artist_type ON artist_type.id = artist.type
    JOIN area ON area.id = artist.area
    WHERE artist_type.name = 'Group'
    AND area.name = 'United States'
    AND artist.begin_date_year IS NOT NULL
    AND CAST(first_three_digits AS INTEGER) > 189
    GROUP BY LEFT(CAST(artist.begin_date_year AS CHAR), 3)
)
SELECT 
    first_three_digits || '0s' AS decade,
    group_count
FROM artist_decades
ORDER BY first_three_digits;

-- -- WITH LEFT(CAST(artist.begin_date_year AS CHAR), 3) AS first_three_digits
-- SELECT 
-- LEFT(CAST(artist.begin_date_year AS CHAR), 3) AS first_three_digits,
-- first_three_digits || '0s' AS decade,
-- COUNT(*) AS group_count
-- -- SELECT artist.name, artist.begin_date_year
-- FROM artist
-- JOIN artist_type ON artist_type.id = artist.type
-- JOIN area ON area.id = artist.area
-- WHERE artist_type.name = 'Group'
-- AND area.name = 'United States'
-- AND CAST(first_three_digits AS INTEGER) > 189
-- GROUP BY first_three_digits
-- ORDER BY first_three_digits



--.read q6_us_artist_groups_per_decade.sqlite.sql
--result
D .read q6_us_artist_groups_per_decade.sqlite.sql
┌─────────┬─────────────┐
│ decade  │ group_count │
│ varchar │    int64    │
├─────────┼─────────────┤
│ 1900s   │           6 │
│ 1910s   │          15 │
│ 1920s   │          63 │
│ 1930s   │          69 │
│ 1940s   │         102 │
│ 1950s   │         356 │
│ 1960s   │        1304 │
│ 1970s   │         989 │
│ 1980s   │        2215 │
│ 1990s   │        4545 │
│ 2000s   │        5788 │
│ 2010s   │        2547 │
│ 2020s   │          18 │
├─────────┴─────────────┤
│ 13 rows     2 columns │
└───────────────────────┘
Run Time (s): real 0.017 