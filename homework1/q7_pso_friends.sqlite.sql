WITH cooperate_artist AS (
    SELECT artist_credit_name.artist_credit as credit
    FROM artist_credit_name
    JOIN artist ON artist.id = artist_credit_name.artist
    WHERE artist.name = 'Pittsburgh Symphony Orchestra'
)
SELECT artist.name
FROM cooperate_artist
JOIN artist_credit_name ON artist_credit_name.artist_credit = cooperate_artist.credit
JOIN artist ON artist.id = artist_credit_name.artist
WHERE cooperate_artist.credit IS NOT NULL
AND artist.name != 'Pittsburgh Symphony Orchestra'
ORDER BY credit ASC;

--.read q7_pso_friends.sqlite.sql
--result
D .read q7_pso_friends.sqlite.sql
┌────────────────────────────┐
│            name            │
│          varchar           │
├────────────────────────────┤
│ John Williams              │
│ Itzhak Perlman             │
│ Boston Pops Orchestra      │
│ John Williams              │
│ Itzhak Perlman             │
│ Ottorino Respighi          │
│ Lorin Maazel               │
│ Lorin Maazel               │
│ Jean Sibelius              │
│ Сергей Сергеевич Прокофьев │
│ Yo‐Yo Ma                   │
│ Пётр Ильич Чайковский      │
│ Lorin Maazel               │
│ Сергей Сергеевич Прокофьев │
│ Yo‐Yo Ma                   │
│ Пётр Ильич Чайковский      │
│ Lorin Maazel               │
│ Christian Sinding          │
│ André Previn               │
│ Jean Sibelius              │
│       ·                    │
│       ·                    │
│       ·                    │
│ Maurice Ravel              │
│ André Previn               │
│ Camille Saint‐Saëns        │
│ Manfred Honeck             │
│ Anton Bruckner             │
│ Rolf Zuckowski             │
│ André Previn               │
│ Fritz Reiner               │
│ Rudolf Serkin              │
│ Johannes Brahms            │
│ André Previn               │
│ Karl Goldmark              │
│ Pablo de Sarasate          │
│ Itzhak Perlman             │
│ Jonathan Leshnoff          │
│ Пётр Ильич Чайковский      │
│ Manfred Honeck             │
│ Michael Rusinek            │
│ Nancy Goeres               │
│ Manfred Honeck             │
├────────────────────────────┤
│    254 rows (40 shown)     │
└────────────────────────────┘
Run Time (s): real 0.035 user 0.256690 sys 0.000000