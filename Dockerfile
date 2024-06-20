FROM ubuntu:24.04
WORKDIR /cmu-15445
COPY . .

# 更新包列表并安装 SQLite 和其他必要的工具
RUN apt-get update && apt-get install -y \
    sqlite3 \
    wget \
    unzip \
    build-essential \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# 安装duckdb
RUN wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip \
    && mv duckdb /usr/local/bin/ \
    && rm duckdb_cli-linux-amd64.zip

# 下载数据库文件
RUN wget https://15445.courses.cs.cmu.edu/spring2024/files/musicbrainz-cmudb2024.db.gz \
    && echo "e0c1d36c1ab4cf5b13afff520b4554a2  musicbrainz-cmudb2024.db.gz" | md5sum -c - \
    && gunzip musicbrainz-cmudb2024.db.gz

# 创建占位符目录及文件
RUN mkdir placeholder && cd placeholder && touch \
    q1_sample.duckdb.sql \
    q1_sample.sqlite.sql \
    q2_solo_artist.duckdb.sql \
    q2_solo_artist.sqlite.sql \
    q3_german_female_artists.duckdb.sql \
    q3_german_female_artists.sqlite.sql \
    q4_longest_name.duckdb.sql \
    q4_longest_name.sqlite.sql \
    q5_oldest_releases.duckdb.sql \
    q5_oldest_releases.sqlite.sql \
    q6_same_year_releases.duckdb.sql \
    q6_same_year_releases.sqlite.sql \
    q7_pso_friends.duckdb.sql \
    q7_pso_friends.sqlite.sql \
    q8_symphony_orchestra.duckdb.sql \
    q8_symphony_orchestra.sqlite.sql \
    q9_non_us_release_per_decade.duckdb.sql \
    q9_non_us_release_per_decade.sqlite.sql \
    q10_canadian_will.duckdb.sql \
    q10_canadian_will.sqlite.sql \
    && cd ..

# RUN mv musicbrainz-cmudb2024.db /cmu-15445


# # 加载数据库并创建索引
# RUN sqlite3 imdb-cmudb2022.db <<EOF
# CREATE INDEX ix_people_name ON people (name);
# CREATE INDEX ix_titles_type ON titles (type);
# CREATE INDEX ix_titles_primary_title ON titles (primary_title);
# CREATE INDEX ix_titles_original_title ON titles (original_title);
# CREATE INDEX ix_akas_title_id ON akas (title_id);
# CREATE INDEX ix_akas_title ON akas (title);
# CREATE INDEX ix_crew_title_id ON crew (title_id);
# CREATE INDEX ix_crew_person_id ON crew (person_id);
# EOF

# 设置默认命令为 bash
CMD ["/bin/bash"]

# build command
# docker build --network=host -t <imagename> .
# run container
# docker run -it --rm sqlite_homework
