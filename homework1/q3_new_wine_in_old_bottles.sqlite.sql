WITH RankedResults AS (
    SELECT release.name AS release_name,
    artist.name AS artist_name,
    release_info.date_year,
    release_info.date_year || '-' || 
    printf('%02d', release_info.date_month) || '-' || 
    printf('%02d', release_info.date_day) AS full_date,
    ROW_NUMBER() OVER (PARTITION BY release.name ORDER BY release_info.date_year, release_info.date_month, release_info.date_day DESC) AS rank
    FROM release
    JOIN medium ON release.id = medium.release
    JOIN medium_format ON medium_format.id = medium.format
    JOIN release_info ON release_info.release = release.id
    JOIN artist_credit ON artist_credit.id = release.artist_credit
    JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
    JOIN artist ON artist.id = artist_credit_name.artist
    -- JOIN area ON area.id = release_info.area
    -- WHERE artist.name = 'The Beatles' 
    -- AND medium_format.name = '12" Vinyl' 
    -- AND area.name = 'United Kingdom'
    WHERE medium_format.name = 'Cassette'
)
SELECT release_name, artist_name, date_year
FROM RankedResults
WHERE rank = 1
ORDER BY full_date DESC, release_name ASC, artist_name ASC
LIMIT 10;

--.read q3_new_wine_in_old_bottles.sqlite.sql
-- result
sqlite> .read q3_new_wine_in_old_bottles.sqlite.sql        
Lesions of a Different Kind|Undeath|2020
Heaven & Hell|Ava Max|2020
Women in Music, Pt. III|HAIM|2020
Flashback|Haunt|2020
Moonth|Merce Lemon|2020
PUNANI|6ix9ine|2020
Deep Dyed|Deep Dyed|2020
Meurtrières|Meurtrières|2020
Moondog Vol.1|Dan Peters|2020
Nigths And Profecy|Fossil Aerosol Mining Project|2020

Run Time: real 9.084 user 3.603147 sys 1.153714
sqlite>