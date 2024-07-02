.timer on
WITH release_1950 AS (
    SELECT language.name,
    COUNT(*) AS language_count
    FROM release
    JOIN release_info ON release_info.release = release.id
    JOIN language ON language.id = release.language
    WHERE release_info.date_year > 1949
    AND release_info.date_year < 1960
    GROUP BY language.name
), release_2010 AS (
    SELECT language.name,
    COUNT(*) AS language_count
    FROM release
    JOIN release_info ON release_info.release = release.id
    JOIN language ON language.id = release.language
    WHERE release_info.date_year > 2009
    AND release_info.date_year < 2020
    GROUP BY language.name
), combined AS (
    SELECT release_1950.name,
    release_1950.language_count AS language_count_1950,
    COALESCE(release_2010.language_count, 0) AS language_count_2010
    FROM release_1950
    LEFT JOIN release_2010 ON release_1950.name = release_2010.name
    UNION
    SELECT release_2010.name,
    COALESCE(release_1950.language_count, 0) AS language_count_1950,
    release_2010.language_count AS language_count_2010
    FROM release_2010
    LEFT JOIN release_1950 ON release_1950.name = release_2010.name
), percentage AS (
    SELECT name,
    language_count_1950,
    language_count_2010,
    language_count_1950 * 1.0 / (SELECT SUM(language_count_1950) FROM combined) AS percentage_1950,
    language_count_2010 * 1.0 / (SELECT SUM(language_count_2010) FROM combined) AS percentage_2010,
    (language_count_2010 * 1.0 / (SELECT SUM(language_count_2010) FROM combined)) - (language_count_1950 * 1.0 / (SELECT SUM(language_count_1950) FROM combined)) AS increase
    FROM combined
    WHERE increase > 0
)
SELECT name,
language_count_1950,
language_count_2010,
ROUND(increase, 3) AS increase
-- increase
FROM percentage
ORDER BY increase DESC
;

--.read q9_music_in_the_world.sqlite.sql
--result
D .read q9_music_in_the_world.sqlite.sql
┌──────────────────────┬─────────────────────┬─────────────────────┬──────────┐
│         name         │ language_count_1950 │ language_count_2010 │ increase │
│       varchar        │        int64        │        int64        │  double  │
├──────────────────────┼─────────────────────┼─────────────────────┼──────────┤
│ Japanese             │                  21 │               57240 │    0.033 │
│ Russian              │                   8 │               29199 │    0.017 │
│ [Multiple languages] │                 113 │               45401 │    0.015 │
│ Mandarin Chinese     │                   0 │               16849 │    0.011 │
│ Spanish              │                 152 │               44351 │     0.01 │
│ German               │                  99 │               31018 │    0.008 │
│ Chinese              │                   4 │               12430 │    0.007 │
│ Polish               │                   0 │                2648 │    0.002 │
│ Korean               │                   0 │                3754 │    0.002 │
│ Indonesian           │                   0 │                 975 │    0.001 │
│ Norwegian            │                   0 │                1463 │    0.001 │
│ Danish               │                   3 │                1383 │    0.001 │
│ Czech                │                   0 │                 869 │    0.001 │
│ Arabic               │                   1 │                2205 │    0.001 │
│ Turkish              │                   0 │                1368 │    0.001 │
│ Hungarian            │                   0 │                 908 │    0.001 │
│ Punjabi              │                   0 │                1537 │    0.001 │
│ Breton               │                   0 │                2080 │    0.001 │
│ German, Swiss        │                   1 │                 482 │      0.0 │
│ Tigrinya             │                   0 │                   4 │      0.0 │
│    ·                 │                   · │                   · │       ·  │
│    ·                 │                   · │                   · │       ·  │
│    ·                 │                   · │                   · │       ·  │
│ Western Arrarnta     │                   0 │                   2 │      0.0 │
│ Gujarati             │                   0 │                   4 │      0.0 │
│ Swati                │                   0 │                   1 │      0.0 │
│ Burmese              │                   0 │                  13 │      0.0 │
│ Duala                │                   0 │                   1 │      0.0 │
│ Cebuano              │                   0 │                   2 │      0.0 │
│ Gothic               │                   0 │                   1 │      0.0 │
│ Ume Sami             │                   0 │                   1 │      0.0 │
│ Luo                  │                   0 │                   2 │      0.0 │
│ Kölsch               │                   0 │                  41 │      0.0 │
│ Manx                 │                   0 │                   7 │      0.0 │
│ Scots                │                   0 │                   6 │      0.0 │
│ Bambara              │                   0 │                  19 │      0.0 │
│ Divehi               │                   0 │                   5 │      0.0 │
│ Ladin                │                   0 │                   5 │      0.0 │
│ Montagnais           │                   0 │                  13 │      0.0 │
│ Mapudungun           │                   0 │                   1 │      0.0 │
│ Kongo                │                   0 │                   2 │      0.0 │
│ Shona                │                   0 │                   4 │      0.0 │
│ Kannada              │                   0 │                   9 │      0.0 │
├──────────────────────┴─────────────────────┴─────────────────────┴──────────┤
│ 203 rows (40 shown)                                               4 columns │
└─────────────────────────────────────────────────────────────────────────────┘
Run Time (s): real 0.836 user 7.934667 sys 0.444082
D 