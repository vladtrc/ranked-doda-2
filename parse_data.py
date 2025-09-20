import re
from dataclasses import dataclass
from typing import List
from datetime import datetime


@dataclass
class PlayerStats:
    player_name: str
    team: str
    position: int
    net_worth: int
    kills: int
    deaths: int
    assists: int


@dataclass
class Match:
    date_time: datetime
    duration: str  # raw, e.g. "46:30" or "1:12:05"
    duration_sec: int
    radiant_kills: int
    dire_kills: int
    winning_team: str
    players: List[PlayerStats]


RE_DT_FLEX = re.compile(r'^(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})$')
RE_MATCH_INFO = re.compile(
    r'^(?P<duration>(?:\d{1,2}:)?\d{1,2}:\d{2})\s+(?P<rk>\d+)-(?P<dk>\d+)\s+(?P<winner>radiant|dire)$'
)
RE_PLAYER = re.compile(
    r'^(?P<name>\S+)\s+(?P<pos>\d+)\s+(?P<net>\d+)\s+(?P<k>\d+)/(?P<d>\d+)/(?P<a>\d+)$'
)


def split_into_blocks(text: str) -> List[str]:
    blocks, current = [], []
    for line in text.splitlines():
        stripped = re.sub(r'//.*', '', line).strip()
        if not stripped:
            if current:
                blocks.append("\n".join(current))
                current = []
        else:
            current.append(stripped)
    if current:
        blocks.append("\n".join(current))
    return blocks


def parse_dt_flex(s: str) -> datetime:
    m = RE_DT_FLEX.match(s.strip())
    if not m:
        raise ValueError(f"bad datetime '{s}'")
    date, h, mm = m.groups()
    return datetime.strptime(f"{date} {int(h):02d}:{mm}", "%Y-%m-%d %H:%M")


def parse_duration_seconds(s: str) -> int:
    parts = s.split(':')
    if len(parts) == 2:
        m, sec = map(int, parts)
        h = 0
    elif len(parts) == 3:
        h, m, sec = map(int, parts)
    else:
        raise ValueError(f"bad duration '{s}'")
    return h * 3600 + m * 60 + sec


def parse_block(block: str) -> Match:
    rows = [r.strip() for r in block.split('\n') if r.strip()]
    if len(rows) < 3:
        raise ValueError("block too short")

    date_time = parse_dt_flex(rows[0])

    m = RE_MATCH_INFO.match(rows[1])
    if not m:
        raise ValueError(f"bad match info '{rows[1]}' (expected '[[H:]MM:SS] RK-DK winner')")

    duration = m.group('duration')
    duration_sec = parse_duration_seconds(duration)
    radiant_kills = int(m.group('rk'))
    dire_kills = int(m.group('dk'))
    winning_team = m.group('winner')

    players: List[PlayerStats] = []
    current_team = None
    for raw in rows[2:]:
        if raw in ('radiant', 'dire'):
            current_team = raw
            continue
        if not current_team:
            raise ValueError(f"player before team marker: '{raw}'")
        pm = RE_PLAYER.match(raw)
        if not pm:
            raise ValueError(f"bad player line '{raw}' (expected 'name pos net k/d/a')")
        players.append(PlayerStats(
            player_name=pm.group('name'),
            team=current_team,
            position=int(pm.group('pos')),
            net_worth=int(pm.group('net')),
            kills=int(pm.group('k')),
            deaths=int(pm.group('d')),
            assists=int(pm.group('a')),
        ))

    return Match(
        date_time=date_time,
        duration=duration,
        duration_sec=duration_sec,
        radiant_kills=radiant_kills,
        dire_kills=dire_kills,
        winning_team=winning_team,
        players=players
    )


def parse_dota_file(filename: str) -> List[Match]:
    with open(filename, 'r') as f:
        content = f.read()

    matches: List[Match] = []
    success = fail = 0
    for block in split_into_blocks(content):
        try:
            matches.append(parse_block(block))
            success += 1
        except Exception as e:
            fail += 1
            print("=== Failed to parse block ===")
            print(block)
            print(f"Error: {e}\n")

    total = success + fail
    print(f"Parsed {success}/{total} matches successfully")
    return matches
