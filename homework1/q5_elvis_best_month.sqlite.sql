.timer on
WITH artist_monthly_counts AS (
    SELECT artist.name, release_info.date_month, COUNT(*) AS month_count,
           ROW_NUMBER() OVER (PARTITION BY artist.name ORDER BY COUNT(*) DESC) AS rn
    FROM artist
    JOIN artist_type ON artist_type.id = artist.type
    JOIN artist_credit_name ON artist_credit_name.artist = artist.id
    JOIN artist_credit ON artist_credit.id = artist_credit_name.artist_credit
    JOIN release ON release.artist_credit = artist_credit.id
    JOIN release_info ON release_info.release = release.id
    WHERE artist.name LIKE 'Elvis%'
    AND artist_type.name = 'Person'
    AND release_info.date_month IS NOT NULL
    GROUP BY artist.name, release_info.date_month
)
SELECT name, date_month, month_count
FROM artist_monthly_counts
WHERE rn = 1
ORDER BY month_count DESC, name ASC;

-- SELECT artist.name, release_info.date_month, COUNT(*) AS month_count
-- FROM artist
-- -- JOIN artist_alias ON artist_alias.artist = artist.id
-- JOIN artist_type ON artist_type.id = artist.type
-- JOIN artist_credit_name ON artist_credit_name.artist = artist.id
-- JOIN artist_credit ON artist_credit.id = artist_credit_name.artist_credit
-- JOIN release ON release.artist_credit = artist_credit.id
-- JOIN release_info ON release_info.release = release.id
-- WHERE artist.name LIKE 'Elvis%'
-- -- WHERE (artist.name LIKE 'Elvis%' OR artist_alias.name LIKE 'Elvis%')
-- AND artist_type.name = 'Person'
-- AND release_info.date_month IS NOT NULL
-- GROUP BY artist.name, release_info.date_month
-- HAVING COUNT(*) = (
--     SELECT MAX(monthly_count)
--     FROM (
--         SELECT COUNT(*) AS monthly_count
--         FROM artist
--         JOIN artist_type ON artist_type.id = artist.type
--         JOIN artist_credit_name ON artist_credit_name.artist = artist.id
--         JOIN artist_credit ON artist_credit.id = artist_credit_name.artist_credit
--         JOIN release ON release.artist_credit = artist_credit.id
--         JOIN release_info ON release_info.release = release.id
--         WHERE artist.name LIKE 'Elvis%'
--         AND artist_type.name = 'Person'
--         AND release_info.date_month IS NOT NULL
--         GROUP BY artist.name, release_info.date_month
--     ) AS subquery
-- )
-- ORDER BY artist.name ASC, release.id ASC
-- ;

--.read q5_elvis_best_month.sqlite.sql

--result
sqlite> .read q5_elvis_best_month.sqlite.sql
Elvis Presley|10|57
Elvis Costello|9|21
Elvis Crespo|5|9
Elvis Depressedly|10|3
Elvis Nick|9|2
Elvis Perkins|3|2
Elvis Blue|9|1
Elvis Magno|2|1
Elvis Martínez|5|1
Elvis McFly|10|1