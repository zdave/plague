import collections
import gspread_asyncio
from oauth2client.service_account import ServiceAccountCredentials
import os
import re

import common

## Parsing

_num_re = re.compile('[0-9]+')

def _nums(s):
    for m in _num_re.finditer(s):
        yield int(m.group())

def _max_num(s):
    return max(_nums(s), default=None)

class Range:
    def __init__(self, low, high):
        assert (low is None) or (high is None) or (low <= high)
        self.low = low
        self.high = high

    def __contains__(self, x):
        if self.low is None:
            if self.high is None:
                return True
            return x <= self.high
        if self.high is None:
            return x >= self.low
        return self.low <= x <= self.high

    def __str__(self):
        if self.low is None:
            if self.high is None:
                return 'any'
            return f'up to {self.high}'
        if self.high is None:
            return f'{self.low}+'
        if self.low == self.high:
            return f'{self.low}'
        return f'{self.low}..{self.high}'

_num_plus_re = re.compile(r'([0-9]+)\+')

def _num_range(s):
    m = _num_plus_re.search(s)
    if m:
        return Range(int(m.group(1)), None)

    nums = list(_nums(s))
    if not nums:
        return None
    return Range(min(nums), max(nums))

_Field = collections.namedtuple('_Field',
    'name heading_re parse sub_heading_row required',
    defaults=(lambda s: s, None, False,))

_fields = [
    _Field('title', re.compile('title', re.IGNORECASE), required=True),
    _Field('max_players', re.compile('max.+player', re.IGNORECASE), _max_num),
    _Field('good_players', re.compile('good.+player', re.IGNORECASE), _num_range),
    _Field('owns', re.compile('who.+owns', re.IGNORECASE), bool, sub_heading_row=2)]

_heading_row = 0
_data_begin_row = 3

Game = collections.namedtuple('Game', (field.name for field in _fields))

def _fixup(game):
    if game.good_players is None:
        return game

    if ((game.good_players.low is not None) and (game.max_players is not None) and
            (game.good_players.low > game.max_players)):
        # Assume we didn't parse the good players field properly...
        return game._replace(good_players=None)

    low = game.good_players.low
    if (low is not None) and (low <= 1):
        low = None

    high = game.good_players.high
    if game.max_players is not None:
        if (high is not None) and (high >= game.max_players):
            high = None
        if (high is None) and (low is not None) and (low == game.max_players):
            high = low

    return game._replace(good_players=Range(low, high))

class _Loc:
    def __init__(self, field):
        self.field = field
        self.begin_col = None
        # self.end_col set once found
        # self.sub_headings set at end of _get_locs

def _get_locs(rows):
    locs = {field.name: _Loc(field) for field in _fields}
    cont_loc = None
    for col, heading in enumerate(rows[_heading_row]):
        if heading:
            cont_loc = None
            for loc in locs.values():
                heading_re = loc.field.heading_re
                if heading_re.search(heading):
                    if loc.begin_col is not None:
                        raise common.Error(
                            f'I found multiple headings matching "{heading_re.pattern}" in the GL spreadsheet.')
                    loc.begin_col = col
                    loc.end_col = col + 1
                    if loc.field.sub_heading_row is not None:
                        cont_loc = loc
                    break
        elif cont_loc is not None:
            if rows[cont_loc.field.sub_heading_row][col]:
                cont_loc.end_col = col + 1
            else:
                cont_loc = None

    for loc in locs.values():
        if loc.begin_col is None:
            raise common.Error(
                f'I couldn\'t find a heading matching "{loc.field.heading_re.pattern}" in the GL spreadsheet.')

        if loc.field.sub_heading_row is not None:
            loc.sub_headings = rows[loc.field.sub_heading_row][loc.begin_col:loc.end_col]

            seen = set()
            for sub_heading in loc.sub_headings:
                if sub_heading in seen:
                    raise common.Error(
                        'There are multiple columns in the GL spreadsheet under '
                        f'{rows[_heading_row][loc.begin_col]} with the same '
                        f'sub-heading ({sub_heading}).')
                seen.add(sub_heading)

    return locs

class _MissingRequiredField(Exception):
    pass

def _parse_field(loc, row):
    values = []
    for s in row[loc.begin_col:loc.end_col]:
        if loc.field.required and not s:
            raise _MissingRequiredField()
        values.append(loc.field.parse(s))

    return values[0] if loc.field.sub_heading_row is None else dict(zip(loc.sub_headings, values))

GameList = collections.namedtuple('GameList', 'names games')

def _parse(rows):
    rows = [[s.strip() for s in row] for row in rows]

    locs = _get_locs(rows)

    names = set(locs['owns'].sub_headings)

    games = []
    for row in rows[_data_begin_row:]:
        try:
            values = {loc.field.name: _parse_field(loc, row) for loc in locs.values()}
        except _MissingRequiredField:
            continue
        games.append(_fixup(Game(**values)))

    return GameList(names, games)

## Google Sheets stuff

def _make_gsheets_client_manager(gsheets_creds):
    def get_creds():
        return ServiceAccountCredentials.from_json_keyfile_name(
            gsheets_creds, ['https://www.googleapis.com/auth/spreadsheets.readonly'])

    return gspread_asyncio.AsyncioGspreadClientManager(get_creds)

async def _fetch_gsheets_rows(cm, id):
    c = await cm.authorize()
    ss = await c.open_by_key(id)
    ws = await ss.get_worksheet(0)
    return await ws.get_all_values()

## Top level

class Session:
    def __init__(self, gsheets_creds):
        self.__cm = _make_gsheets_client_manager(gsheets_creds)

    async def fetch(self, id):
        rows = await _fetch_gsheets_rows(self.__cm, id)
        return _parse(rows)
