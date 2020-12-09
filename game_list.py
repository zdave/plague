import collections
import functools
import gspread_asyncio
from oauth2client.service_account import ServiceAccountCredentials
import re

import common

## Range & co

@functools.total_ordering
class _Least:
    # Default __eq__ is fine assuming there is only one instance

    def __gt__(self, other):
        return False

least = _Least()

@functools.total_ordering
class _Greatest:
    # Default __eq__ is fine assuming there is only one instance

    def __lt__(self, other):
        return False

greatest = _Greatest()

def _round_low_to_multiple(low, multiple_of):
    return low if low is least else low + (-low % multiple_of)

def _round_high_to_multiple(high, multiple_of):
    return high if high is greatest else high - (high % multiple_of)

class Range:
    def __init__(self, low, high, multiple_of=1):
        assert (low is least) or isinstance(low, int)
        assert (high is greatest) or isinstance(high, int)
        assert isinstance(multiple_of, int)

        self.low = _round_low_to_multiple(low, multiple_of)
        self.high = _round_high_to_multiple(high, multiple_of)
        self.multiple_of = multiple_of

    def __bool__(self):
        return self.low <= self.high

    def __contains__(self, x):
        return (self.low <= x <= self.high) and ((x % self.multiple_of) == 0)

    def __str__(self):
        if not self:
            return 'none'

        if self.low is least:
            if self.high is greatest:
                s = 'any'
            else:
                s = f'up to {self.high}'
        elif self.high is greatest:
            s = f'{self.low}+'
        elif self.low == self.high:
            return f'{self.low}'
        else:
            s = f'{self.low}..{self.high}'

        if self.multiple_of == 1:
            return s
        if self.multiple_of == 2:
            return f'{s} even'
        return f'{s} multiple of {self.multiple_of}'

    def simplified(self, implicit_low, implicit_high):
        assert (implicit_low is least) or isinstance(implicit_low, int)
        assert (implicit_high is greatest) or isinstance(implicit_high, int)

        implicit_low = _round_low_to_multiple(implicit_low, self.multiple_of)
        implicit_high = _round_high_to_multiple(implicit_high, self.multiple_of)

        low = max(self.low, implicit_low)
        high = min(self.high, implicit_high)

        if low >= high:
            # Either empty or one number in range
            return Range(low, high)

        return Range(
            least if low == implicit_low else low,
            greatest if high == implicit_high else high,
            self.multiple_of)

## Parsing

_num_re = re.compile('[0-9]+')

def _nums(s):
    for m in _num_re.finditer(s):
        yield int(m.group())

def _max_num(s):
    return max(_nums(s), default=None)

_num_plus_re = re.compile(r'([0-9]+)\+')

def _num_range(s):
    multiple_of = 2 if 'even' in s.lower() else 1

    if m := _num_plus_re.search(s):
        return Range(int(m.group(1)), greatest, multiple_of)

    if nums := list(_nums(s)):
        return Range(min(nums), max(nums), multiple_of)

    return None

_Field = collections.namedtuple('_Field',
    'name heading_re parse sub_heading_row required',
    defaults=(lambda s: s, None, False))

_fields = [
    _Field('title', re.compile('title', re.IGNORECASE), required=True),
    _Field('platform', re.compile('platform', re.IGNORECASE)),
    _Field('max_players', re.compile('max.+player', re.IGNORECASE), _max_num),
    _Field('good_players', re.compile('good.+player', re.IGNORECASE), _num_range),
    _Field('owns', re.compile('who.+owns', re.IGNORECASE), bool, sub_heading_row=2)]

_heading_row = 0
_data_begin_row = 3

Game = collections.namedtuple('Game', (field.name for field in _fields))

def _fixup(game):
    if game.good_players is None:
        return game

    if good_players := game.good_players.simplified(1,
            greatest if game.max_players is None else game.max_players):
        return game._replace(good_players=good_players)

    # Assume we didn't parse the good players field properly...
    return game._replace(good_players=None)

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
