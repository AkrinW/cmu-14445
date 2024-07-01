WITH RankedResults AS (
    SELECT release.name,
    release_info.date_year,
    release_info.date_year || '-' || 
    printf('%02d', release_info.date_month) || '-' || 
    printf('%02d', release_info.date_day) AS full_date,
    ROW_NUMBER() OVER (PARTITION BY release.name ORDER BY release_info.date_year, release_info.date_month, release_info.date_day ASC) AS rank
    FROM release
    JOIN medium ON release.id = medium.release
    JOIN medium_format ON medium_format.id = medium.format
    JOIN release_info ON release_info.release = release.id
    JOIN artist_credit ON artist_credit.id = release.artist_credit
    JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
    JOIN artist ON artist.id = artist_credit_name.artist
    JOIN area ON area.id = release_info.area
    WHERE artist.name = 'The Beatles' 
    AND medium_format.name = '12" Vinyl' 
    AND area.name = 'United Kingdom'
)
SELECT name, date_year
FROM RankedResults
WHERE rank = 1 AND full_date < '1970-04-10'
ORDER BY full_date ASC, name ASC;

--.read q2_beatles_uk_releases.duckdb.sql
-- result
┌───────────────────────────────────────┬───────────┐
│                 name                  │ date_year │
│                varchar                │   int64   │
├───────────────────────────────────────┼───────────┤
│ Please Please Me                      │      1963 │
│ With The Beatles                      │      1963 │
│ A Hard Day’s Night                    │      1964 │
│ Beatles for Sale                      │      1964 │
│ Help!                                 │      1965 │
│ Rubber Soul                           │      1965 │
│ Revolver                              │      1966 │
│ A Collection of Beatles Oldies        │      1966 │
│ Sgt. Pepper’s Lonely Hearts Club Band │      1967 │
│ The Beatles' First                    │      1967 │
│ Magical Mystery Tour                  │      1967 │
│ The Beatles                           │      1968 │
│ Abbey Road                            │      1969 │
├───────────────────────────────────────┴───────────┤
│ 13 rows                                 2 columns │
└───────────────────────────────────────────────────┘