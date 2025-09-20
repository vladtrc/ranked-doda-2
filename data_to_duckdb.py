from typing import List
import duckdb

from parse_data import Match


def load_matches_into_duckdb(matches: List[Match]) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(database=':memory:')
    conn.execute("""
                 CREATE TABLE matches (
                                          match_id    BIGINT PRIMARY KEY,
                                          date_time   TIMESTAMP,
                                          duration    VARCHAR,
                                          duration_sec INTEGER,
                                          radiant_kills INTEGER,
                                          dire_kills    INTEGER,
                                          winning_team  VARCHAR
                 );
                 """)
    conn.execute("""
                 CREATE TABLE players (
                                          match_id   BIGINT,
                                          player_name VARCHAR,
                                          team        VARCHAR,
                                          position    INTEGER,
                                          net_worth   INTEGER,
                                          kills       INTEGER,
                                          deaths      INTEGER,
                                          assists     INTEGER
                 );
                 """)

    # Prepare rows
    match_rows = []
    player_rows = []
    for mid, m in enumerate(matches, start=1):
        match_rows.append((
            mid,
            m.date_time,
            m.duration,
            m.duration_sec,
            m.radiant_kills,
            m.dire_kills,
            m.winning_team,
        ))
        for p in m.players:
            player_rows.append((
                mid,
                p.player_name,
                p.team,
                p.position,
                p.net_worth,
                p.kills,
                p.deaths,
                p.assists,
            ))

    # Bulk insert
    conn.executemany("""
                     INSERT INTO matches
                     (match_id, date_time, duration, duration_sec, radiant_kills, dire_kills, winning_team)
                     VALUES (?, ?, ?, ?, ?, ?, ?)
                     """, match_rows)

    conn.executemany("""
                     INSERT INTO players
                     (match_id, player_name, team, position, net_worth, kills, deaths, assists)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                     """, player_rows)

    # Useful indexes
    conn.execute("CREATE INDEX players_match_idx ON players(match_id);")
    conn.execute("CREATE INDEX players_name_idx ON players(player_name);")

    return conn
