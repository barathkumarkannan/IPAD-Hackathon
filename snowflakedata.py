from pyspark.sql import SparkSession
import pandas as pd

import configparser
import sys

#### Python sql query builder

import functools
import textwrap
from collections import defaultdict
from typing import NamedTuple

import configparser

class Query:
    keywords = [
        'WITH',
        'SELECT',
        'FROM',
        'WHERE',
        'CASE',

        'GROUP BY',
        'HAVING',
        'ORDER BY',
        'LIMIT',

    ]

    separators = dict(WHERE='AND', HAVING='AND')
    default_separator = ','

    formats = (
        defaultdict(lambda: '{value}'),
        defaultdict(lambda: '{value} AS {alias}', WITH='{alias} AS {value}'),
    )

    subquery_keywords = {'WITH'}
    fake_keywords = dict(JOIN='FROM')
    flag_keywords = dict(SELECT={'DISTINCT', 'ALL'})
    fake_keywords = {
        'JOIN': 'FROM',
        'INNER JOIN': 'JOIN',
        'LEFT JOIN': 'JOIN',
        'RIGHT JOIN': 'JOIN',
        # Add more join types here...
    }

    # New join methods
    def _indent(self, text, level=1):
        indentation = ' ' * 4 * level
        return textwrap.indent(text, prefix=indentation)

    def CASE_WHEN(self, expression):
        self._case_expression = expression
        self._case_conditions = []
        return self

    def WHEN(self, condition, result):
        self._case_conditions.append(('WHEN', condition, 'THEN', result))
        return self

    def ELSE(self, result, alias):
        self._case_conditions.append(('ELSE', result, 'END AS', alias))
        return self.add('SELECT', self._build_case_expression(alias))

    def _build_case_expression(self, alias):
        case_expression = f"CASE {self._case_expression}\n"
        for condition in self._case_conditions:
            case_expression += ' '.join(condition) + '\n'
        #         case_expression += f"END AS {alias}"
        return case_expression

    def JOIN(self, *args):
        return self._add_join('JOIN', *args)

    def INNER_JOIN(self, *args):
        return self._add_join('INNER JOIN', *args)

    def LEFT_JOIN(self, *args):
        return self._add_join('LEFT JOIN', *args)

    def RIGHT_JOIN(self, *args):
        return self._add_join('RIGHT JOIN', *args)

    # Helper method for adding join clauses
    def _add_join(self, keyword, *args):
        keyword, fake_keyword = self._resolve_fakes(keyword)
        keyword, flag = self._resolve_flags(keyword)
        target = self.data[keyword]

        if flag:
            if target.flag:
                raise ValueError(f"{keyword} already has flag: {flag!r}")
            target.flag = flag

        kwargs = {}
        if fake_keyword:
            kwargs.update(keyword=fake_keyword)

        for arg in args:
            target.append(_Thing.from_arg(arg, **kwargs))

        return self

    def __init__(self, data=None, separators=None):
        self.data = {}
        if data is None:
            data = dict.fromkeys(self.keywords, ())
        for keyword, args in data.items():
            self.data[keyword] = _FlagList()
            self.add(keyword, *args)

        if separators is not None:
            self.separators = separators

    def add(self, keyword, *args):
        keyword, fake_keyword = self._resolve_fakes(keyword)
        keyword, flag = self._resolve_flags(keyword)
        target = self.data[keyword]

        if flag:
            if target.flag:
                raise ValueError(f"{keyword} already has flag: {flag!r}")
            target.flag = flag

        kwargs = {}
        if fake_keyword:
            kwargs.update(keyword=fake_keyword)
        if keyword in self.subquery_keywords:
            kwargs.update(is_subquery=True)

        for arg in args:
            target.append(_Thing.from_arg(arg, **kwargs))

        return self

    def _resolve_fakes(self, keyword):
        for part, real in self.fake_keywords.items():
            if part in keyword:
                return real, keyword
        return keyword, ''

    def _resolve_flags(self, keyword):
        prefix, _, flag = keyword.partition(' ')
        if prefix in self.flag_keywords:
            if flag and flag not in self.flag_keywords[prefix]:
                raise ValueError(f"invalid flag for {prefix}: {flag!r}")
            return prefix, flag
        return keyword, ''

    def __getattr__(self, name):
        if not name.isupper():
            return getattr(super(), name)
        return functools.partial(self.add, name.replace('_', ' '))

    def __str__(self):
        return ''.join(self._lines())

    def _lines(self):
        for keyword, things in self.data.items():
            if not things:
                continue

            if things.flag:
                yield f'{keyword} {things.flag}\n'
            else:
                yield f'{keyword}\n'

            grouped = [], []
            for thing in things:
                grouped[bool(thing.keyword)].append(thing)
            for group in grouped:
                yield from self._lines_keyword(keyword, group)

    def _lines_keyword(self, keyword, things):
        for i, thing in enumerate(things, 1):
            last = i == len(things)

            if thing.keyword:
                yield thing.keyword + '\n'

            if thing.case:  # Handle CASE statement
                case_expression = self._indent(thing.case)
                yield self._indent(f'CASE {case_expression} END', 1)
            else:
                format = self.formats[bool(thing.alias)][keyword]
                value = thing.value
                if thing.is_subquery:
                    value = f'(\n{self._indent(value)}\n)'
                yield self._indent(format.format(value=value, alias=thing.alias))

            if not last and not thing.keyword:
                try:
                    yield ' ' + self.separators[keyword]
                except KeyError:
                    yield self.default_separator

            yield '\n'


class _Thing(NamedTuple):
    value: str
    alias: str = ''
    keyword: str = ''
    is_subquery: bool = False
    case: str = ''

    @classmethod
    def from_arg(cls, arg, **kwargs):
        if isinstance(arg, str):
            alias, value = '', arg
        elif len(arg) == 2:
            alias, value = arg
        else:
            raise ValueError(f"invalid arg: {arg!r}")
        return cls(_clean_up(value), _clean_up(alias), **kwargs)


class _FlagList(list):
    flag: str = ''


def _clean_up(thing: str) -> str:
    return textwrap.dedent(thing.rstrip()).strip()

#### end of sql query builder

# df=pd.read_excel('C:/Users/dineshka/Desktop/query.xlsx').fillna("")
df=pd.read_excel('C:/Users/dineshka/PycharmProjects/pythonProject2/updated_ingstn_fw/datamapping_sheet.xlsx').fillna("")

query = Query()


def join_operation():
    for i in range(0, len(df)):
        join_type = df["join"][i]
        join_table = df['source_table_name'][i]
        if join_type.upper() == "INNER":
            join_cond = df["on"][i].split(",")

            query.INNER_JOIN(join_table + " " + "ON" + " " + join_cond[0] + "=" + join_cond[1])
        elif join_type.upper() == "LEFT":
            join_cond = df["on"][i].split(",")

            query.LEFT_JOIN(join_table + " " + "ON" + " " + join_cond[0] + "=" + join_cond[1])
        elif join_type.upper() == "RIGHT":
            join_cond = df["on"][i].split(",")

            query.RIGHT_JOIN(join_table + " " + "ON" + " " + join_cond[0] + "=" + join_cond[1])
    return query


def select_operation():
    for i in range(0, len(df)):
        col_value = df['query'][i]
        col_name = df['target_column_name'][i]

        query.SELECT((col_name, col_value))
    return query


def from_operation():
    from_value = df["source_table_name"][0]
    query.FROM(from_value)


def where_operation():
    for i in range(0, len(df)):
        where_value = df["condition"][i]
        if where_value != "":
            query.WHERE(where_value)

def query_processing(spark,base_dir):
    configfilepath = base_dir + "config.ini"
    config = configparser.ConfigParser()
    config.read(configfilepath)
    url = config['SFConfig']['Sfurl']
    user = config['SFConfig']['Sfuser']
    password = config['SFConfig']['Sfpassword']
    database = config['SFConfig']['Sfdatabase']
    schema = config['SFConfig']['Sfschema']
    dbtable = config['SFConfig']['Dbtable']
    warehouse = config['SFConfig']['Sfwarehouse']

    where_operation()
    from_operation()

    select_operation()
    querys = str(query)
    final_query=querys.lower()

    print(final_query)
    df = spark.read.format("net.snowflake.spark.snowflake") \
        .options(sfUrl=url,
                 sfUser=user,
                 sfPassword=password,
                 sfDatabase=database) \
        .option("query", final_query) \
        .option("sfWarehouse", warehouse) \
        .load()

    return  df


