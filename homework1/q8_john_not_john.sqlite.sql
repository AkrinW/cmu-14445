.timer on
WITH alias_concat AS (
    SELECT 
        artist.name, 
        COUNT(*) AS alias_count,
        GROUP_CONCAT(artist_alias.name, ', ') AS aliases
    FROM artist
    JOIN artist_alias ON artist_alias.artist = artist.id
    WHERE artist.name LIKE '%John'
    GROUP BY artist.name
    ORDER BY artist.name ASC
)
SELECT *
FROM alias_concat
WHERE LOWER(aliases) NOT LIKE '%john%'
;


--.read q8_john_not_john.sqlite.sql
--result
D .read q8_john_not_john.sqlite.sql
┌────────────────┬─────────────┬───────────────────────────────────────────────────────────┐
│      name      │ alias_count │                          aliases                          │
│    varchar     │    int64    │                          varchar                          │
├────────────────┼─────────────┼───────────────────────────────────────────────────────────┤
│ Anaïs St. John │           1 │ Anaïs Brown                                               │
│ Andrw John     │           2 │ El Moncarca de la bachata, Juan Andres Gonzalez Alcantara │
│ Barry St. John │           1 │ Elizabeth Thompson                                        │
│ Fred St. John  │           1 │ Frederic L. Hostetler                                     │
│ Jon St. John   │           2 │ JSJ, Duke Nukem                                           │
│ Kate St John   │           1 │ ケイト・セント・ジョン                                    │
│ Mark St. John  │           1 │ Mark Leslie Norton                                        │
│ Mark and John  │           1 │ ＭＡＲＫ ＡＮＤ ＪＯＨＮ                                  │
│ Persian John   │           1 │ P.J.                                                      │
└────────────────┴─────────────┴───────────────────────────────────────────────────────────┘
Run Time (s): real 0.043 user 0.215534 sys 0.002543