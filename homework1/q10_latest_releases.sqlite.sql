.timer on.read q10_latest_releases.sqlite.sql
WITH aritst_list AS (
    SELECT artist.id, 
    artist.name, 
    artist_credit_name.artist_credit as credit,
    -- artist.begin_date_year
    FROM artist
    JOIN artist_credit_name ON artist_credit_name.artist = artist.id
    JOIn gender ON gender.id = artist.gender
    WHERE artist.begin_date_year = 1991
    AND gender.name = 'Male'
), release_list AS (
    SELECT 
    aritst_list.name as artist,
    release.name as release,
    release_info.date_year,
    release_info.date_month,
    release_info.date_day,
    ROW_NUMBER() OVER (
        PARTITION BY aritst_list.name, release_info.date_year, release_info.date_month, release_info.date_day
        ORDER BY release_info.date_year DESC, release_info.date_month DESC, release_info.date_day DESC
    ) AS rn
    FROM release
    JOIN release_info ON release_info.release = release.id
    JOIN artist_credit ON artist_credit.id = release.artist_credit
    JOIN aritst_list ON aritst_list.credit = artist_credit.id
    WHERE artist_credit.artist_count = 4
    ORDER BY aritst_list.name ASC, 
    release_info.date_year DESC,
    release_info.date_month DESC,
    release_info.date_day DESC
), release_without_repeat AS (
    SELECT
    release_list.artist,
    release_list.release,
    release_list.date_year,
    ROW_NUMBER() OVER (
        PARTITION BY release_list.artist
        ORDER BY release_list.date_year DESC, release_list.date_month DESC, release_list.date_day DESC
    ) AS rn2
    FROM release_list
    WHERE release_list.rn = 1
    ORDER BY release_list.artist ASC, 
    release_list.date_year DESC,
    release_list.date_month DESC,
    release_list.date_day DESC
)
SELECT artist, release, date_year
FROM release_without_repeat
WHERE rn2 < 4
;

--.read q10_latest_releases.sqlite.sql
--result
D .read q10_latest_releases.sqlite.sql
┌────────────────────┬────────────────────────────────────────────────────┬───────────┐
│       artist       │                      release                       │ date_year │
│      varchar       │                      varchar                       │   int64   │
├────────────────────┼────────────────────────────────────────────────────┼───────────┤
│ Akim               │ Pa' olvidarte (Panamá remix)                       │      2019 │
│ Ale Mendoza        │ Está pa' mí (remix)                                │      2018 │
│ Ale Mendoza        │ Piloteando la nave (remix)                         │      2017 │
│ Alesso             │ Let Me Go                                          │      2017 │
│ Black Mayonnaise   │ Dick Goddard's Wintery Forest Revisited            │      2013 │
│ Burna Boy          │ Baddest                                            │      2015 │
│ B‐Case             │ Feel It!                                           │      2013 │
│ Carnage            │ Hella Neck                                         │      2020 │
│ Chucho Flash       │ Hecha completa                                     │      2018 │
│ Deorro             │ Shakalaka                                          │      2018 │
│ Domo Genesis       │ Rella                                              │      2012 │
│ Dubzy              │ Hungry for Dis                                     │      2016 │
│ Ed Sheeran         │ Take Me Back to London (remix)                     │      2019 │
│ Ed Sheeran         │ Lay It All on Me (Rudi VIP mix)                    │      2015 │
│ Farruko            │ Celebration                                        │      2019 │
│ Farruko            │ Como soy II                                        │      2019 │
│ Farruko            │ Pa' olvidarte (remix)                              │      2019 │
│ Frenna             │ Culo                                               │      2018 │
│ Ghostemane         │ Death by Dishonor                                  │      2017 │
│ Guru Randhawa      │ Saaho (Tamil) [Original Motion Picture Soundtrack] │      2019 │
│   ·                │     ·                                              │        ·  │
│   ·                │     ·                                              │        ·  │
│   ·                │     ·                                              │        ·  │
│ Quavo              │ 100 Bands                                          │      2019 │
│ Quavo              │ 100 Bands                                          │      2019 │
│ Quavo              │ No Brainer                                         │      2018 │
│ Simi Stylezz       │ Heaven or Hell                                     │      2019 │
│ Stephen            │ Crossfire, Part III                                │      2017 │
│ Steven Malcolm     │ Oh My God (remix)                                  │      2019 │
│ Tyler, the Creator │ Rella                                              │      2012 │
│ VI Seconds         │ Let Me in the Game                                 │      2013 │
│ Young Thug         │ Cheat Code Mode                                    │      2020 │
│ Young Thug         │ Bullets With Names                                 │      2020 │
│ Young Thug         │ Old Town Road (remix)                              │      2019 │
│ Даниил Трифонов    │ Destination Rachmaninov: Arrival                   │      2019 │
│ Даниил Трифонов    │ Destination Rachmaninov: Departure                 │      2018 │
│ Даниил Трифонов    │ Preghiera / Piano Trios                            │      2017 │
│ 斉藤壮馬           │ SSSS.GRIDMAN CHARACTER SONG.1                      │      2018 │
│ 花江夏樹           │ 欲張りDreamer                                      │      2018 │
│ 花江夏樹           │ ヘヴィーオブジェクト キャラクターソング Vol.1      │      2016 │
│ 花江夏樹           │ ミカグラ学園組曲 vol.2 キャラクターソングCD        │      2015 │
│ 그냥노창           │ NO.MERCY (노머시) Part.5                           │      2015 │
│ 주영               │ NO.MERCY (노머시) Part.3                           │      2015 │
├────────────────────┴────────────────────────────────────────────────────┴───────────┤
│ 57 rows (40 shown)                                                        3 columns │
└─────────────────────────────────────────────────────────────────────────────────────┘
Run Time (s): real 0.060 user 0.432015 sys 0.013642