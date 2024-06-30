创建占位符
$ mkdir placeholder
$ cd placeholder
$ touch q1_sample.sqlite.sql \
        q2_beatles_uk_releases.duckdb.sql \
        q2_beatles_uk_releases.sqlite.sql \
        q3_new_wine_in_old_bottles.duckdb.sql \
        q3_new_wine_in_old_bottles.sqlite.sql \
        q4_devil_in_the_details.duckdb.sql \
        q4_devil_in_the_details.sqlite.sql \
        q5_elvis_best_month.sqlite.sql \
        q6_us_artist_groups_per_decade.sqlite.sql \
        q7_pso_friends.sqlite.sql \
        q8_john_not_john.sqlite.sql \
        q9_music_in_the_world.sqlite.sql \
        q10_latest_releases.sqlite.sql

$DBMS = duckdb or sqlite depending on which DBMS you answered that question with

压缩文件
$ zip -j submission.zip placeholder/*.sql

下载数据库
$ wget https://15445.courses.cs.cmu.edu/fall2023/files/musicbrainz-cmudb2023.db.gz
$ md5sum musicbrainz-cmudb2023.db.gz
781dcc4049a1e85ad84061c0060c7801  musicbrainz-cmudb2023.db.gz
$ gunzip musicbrainz-cmudb2023.db.gz

下载sqlite和duckdb
$ apt-get install sqlite
检查sqlite是否正确
$ sqlite3 musicbrainz-cmudb2023.db
SQLite version 3.32.3
Enter ".help" for usage hints.
sqlite> .tables
area                artist_credit_name  medium              release_status
artist              artist_type         medium_format       work
artist_alias        gender              release             work_type
artist_credit       language            release_info
创建索引
CREATE INDEX ix_artist_name ON artist (name);
CREATE INDEX ix_artist_area ON artist (area);
CREATE INDEX ix_artist_credit_name ON artist_credit_name (artist_credit);
CREATE INDEX ix_artist_credit_id ON artist_credit (id);
CREATE INDEX ix_artist_alias ON artist_alias(artist);
CREATE INDEX ix_work_name ON work (name);
CREATE INDEX ix_work_type ON work (type);
CREATE INDEX ix_work_type_name ON work_type (name);
CREATE INDEX ix_release_id ON release (id);
CREATE INDEX ix_release_artist_credit ON release (artist_credit);
CREATE INDEX ix_release_info_release ON release_info (release);
CREATE INDEX ix_medium_release ON medium (release);
CREATE INDEX ix_medium_format_id on medium_format (id);

下载duckdb
wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip

import.sql
```
INSTALL sqlite_scanner;
LOAD sqlite_scanner;
CALL sqlite_attach('musicbrainz-cmudb2023.db');
PRAGMA show_tables;

ALTER VIEW area RENAME TO area_old;
CREATE TABLE area AS SELECT * FROM area_old;
DROP VIEW area_old;

ALTER VIEW artist RENAME TO artist_old;
CREATE TABLE artist AS SELECT * FROM artist_old;
DROP VIEW artist_old;

ALTER VIEW artist_alias RENAME TO artist_alias_old;
CREATE TABLE artist_alias AS SELECT * FROM artist_alias_old;
DROP VIEW artist_alias_old;

ALTER VIEW artist_credit RENAME TO artist_credit_old;
CREATE TABLE artist_credit AS SELECT * FROM artist_credit_old;
DROP VIEW artist_credit_old;

ALTER VIEW artist_credit_name RENAME TO artist_credit_name_old;
CREATE TABLE artist_credit_name AS SELECT * FROM artist_credit_name_old;
DROP VIEW artist_credit_name_old;

ALTER VIEW artist_type RENAME TO artist_type_old;
CREATE TABLE artist_type AS SELECT * FROM artist_type_old;
DROP VIEW artist_type_old;

ALTER VIEW gender RENAME TO gender_old;
CREATE TABLE gender AS SELECT * FROM gender_old;
DROP VIEW gender_old;

ALTER VIEW language RENAME TO language_old;
CREATE TABLE language AS SELECT * FROM language_old;
DROP VIEW language_old;

ALTER VIEW medium RENAME TO medium_old;
CREATE TABLE medium AS SELECT * FROM medium_old;
DROP VIEW medium_old;

ALTER VIEW medium_format RENAME TO medium_format_old;
CREATE TABLE medium_format AS SELECT * FROM medium_format_old;
DROP VIEW medium_format_old;

ALTER VIEW release RENAME TO release_old;
CREATE TABLE release AS SELECT * FROM release_old;
DROP VIEW release_old;

ALTER VIEW release_info RENAME TO release_info_old;
CREATE TABLE release_info AS SELECT * FROM release_info_old;
DROP VIEW release_info_old;

ALTER VIEW release_status RENAME TO release_status_old;
CREATE TABLE release_status AS SELECT * FROM release_status_old;
DROP VIEW release_status_old;

ALTER VIEW work RENAME TO work_old;
CREATE TABLE work AS SELECT * FROM work_old;
DROP VIEW work_old;

ALTER VIEW work_type RENAME TO work_type_old;
CREATE TABLE work_type AS SELECT * FROM work_type_old;
DROP VIEW work_type_old;

SELECT * from duckdb_tables();
```
$ cat import.sql | ./duckdb musicbrainz-cmudb2023.duckdb

D .tables
area                artist_credit_name  medium              release_status    
artist              artist_type         medium_format       work              
artist_alias        gender              release             work_type         
artist_credit       language            release_info

$ ./duckdb musicbrainz-cmudb2023.duckdb