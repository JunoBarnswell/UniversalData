"""List Navicat sources and release only approved, dialect-specific SELECT text."""

import argparse
import hashlib
import json
import logging
from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET


DEFAULT_CONNECTIONS = Path("E:/项目/connections.ncx")
DIALECTS = {"MYSQL": "mysql", "POSTGRESQL": "postgres", "SQLSERVER": "tsql"}
SQLGLOT_WHEEL = "sqlglot-30.33.0-py3-none-any.whl"
WHEEL_PATH = Path(__file__).resolve().parent / "vendor" / SQLGLOT_WHEEL
sys.path.insert(0, str(WHEEL_PATH))

# Match concrete AST node types. Unrecognized clauses and functions fail closed.
READ_ONLY_NODES = frozenset("""
    Select Union Intersect Except With CTE Subquery
    From Join Lateral Table TableAlias Column Identifier Star Alias Distinct
    Where Group Having Order Ordered Limit Offset Fetch LimitOptions
    GroupingSets Cube Rollup Tuple Paren
    And Or Not EQ NEQ GT GTE LT LTE Is Like ILike Escape Between In Exists
    Null Boolean Literal National RawString Placeholder
    Add Sub Mul Div IntDiv Mod Neg
    Case If Cast TryCast DataType DataTypeParam Interval Var
    Window WindowSpec Filter WithinGroup
    Count Sum Avg Min Max Stddev StddevPop StddevSamp Variance VariancePop
    RowNumber Rank DenseRank PercentRank CumeDist Ntile Lag Lead FirstValue LastValue
    Abs Round Floor Ceil Pow Sqrt Coalesce Nullif Greatest Least
    Concat ConcatWs Lower Upper Length Substring Trim Replace Left Right
    CurrentDate CurrentTimestamp CurrentTime Extract DateTrunc TimestampTrunc
    DateAdd DateSub DateDiff TimeToStr StrToDate StrToTime
    StringAgg GroupConcat
""".split())
CAST_TYPES = frozenset("""
    BOOLEAN TINYINT SMALLINT MEDIUMINT INT BIGINT UTINYINT USMALLINT UMEDIUMINT UINT
    UBIGINT DECIMAL NUMERIC FLOAT DOUBLE REAL MONEY SMALLMONEY CHAR NCHAR VARCHAR
    NVARCHAR TEXT BINARY VARBINARY DATE DATETIME DATETIME2 SMALLDATETIME DATETIME64
    TIME TIMESTAMP TIMESTAMPTZ DATETIMEOFFSET UUID UNIQUEIDENTIFIER
""".split())


def read_connections(path):
    root = ET.parse(path).getroot()
    if root.tag != "Connections":
        raise ValueError("文件不是 Navicat 连接清单。")

    connections = root.findall("Connection")
    if not connections:
        raise ValueError("连接清单中没有连接。")
    return connections


def describe_source(connection):
    name = connection.get("ConnectionName", "")
    engine = connection.get("ConnType", "").upper()
    if not name or not engine:
        raise ValueError("连接清单缺少连接名称或数据库类型。")
    key = f"{engine}\0{name}".encode("utf-8")
    return {
        "id": hashlib.sha256(key).hexdigest()[:16],
        "name": name,
        "engine": engine,
        "database": connection.get("Database") or None,
        "supported": engine in DIALECTS,
    }


def match_mysql(connections, metadata):
    if not isinstance(metadata, dict) or not metadata.keys() <= {"host", "port", "database", "username"}:
        raise ValueError("匹配输入仅允许 host、port、database 和可选 username。")
    host = metadata.get("host")
    database = metadata.get("database")
    port = metadata.get("port", 3306)
    username = metadata.get("username")
    if (
        not isinstance(host, str) or not host.strip().rstrip(".")
        or not isinstance(database, str) or not re.fullmatch(r"[\w$-]{1,64}", database)
        or type(port) is not int or not 1 <= port <= 65535
        or (username is not None and (not isinstance(username, str) or not username))
    ):
        raise ValueError("MySQL 连接元数据格式无效。")

    host = host.strip().rstrip(".").casefold()
    matches = []
    for connection in connections:
        source = describe_source(connection)
        if source["engine"] != "MYSQL":
            continue
        if any(connection.get(key, "").casefold() not in {"", "false", "0"}
               for key in ("SSH", "HTTP", "NamedPipe")):
            continue
        registered_host = connection.get("Host", "").strip().rstrip(".").casefold()
        registered_port = connection.get("Port") or "3306"
        if registered_host != host or registered_port != str(port):
            continue
        registered_username = connection.get("UserName")
        matches.append({
            "id": source["id"],
            "name": source["name"],
            "registered_database": source["database"],
            "database_matches": None if source["database"] is None else source["database"] == database,
            "login_matches": None if not username or not registered_username else registered_username == username,
        })
    return {"status": "matched" if matches else "unmatched", "database": database, "matches": matches}


def validate_select(candidate, dialect):
    if not WHEEL_PATH.is_file():
        raise ValueError("缺少随插件提供的 SQL 解析器。")

    from sqlglot import Dialect, ErrorLevel, exp
    from sqlglot.errors import SqlglotError

    logging.getLogger("sqlglot").disabled = True
    sql_dialect = Dialect.get_or_raise(dialect)
    query_types = (exp.Select, exp.SetOperation)
    try:
        tokens = sql_dialect.tokenize(candidate)
        if any(token.comments for token in tokens):
            raise ValueError("查询须移除 SQL 注释及注释形式的提示。")
        statements = sql_dialect.parse(candidate, error_level=ErrorLevel.IMMEDIATE)
        if len(statements) != 1 or not isinstance(statements[0], query_types):
            raise ValueError("仅接受单条只读 SELECT。")

        query = statements[0]
        for node in query.walk():
            if type(node).__name__ not in READ_ONLY_NODES:
                raise ValueError("查询包含未获准的子句、函数或操作。")
            if isinstance(node, exp.Select) and not node.expressions:
                raise ValueError("SELECT 须指定结果列。")
            if isinstance(node, exp.Table) and not isinstance(node.this, exp.Identifier):
                raise ValueError("查询的表引用须为当前数据源中的标识符。")
            if isinstance(node, exp.DataType) and node.this.name not in CAST_TYPES:
                raise ValueError("类型转换须使用受支持的内置类型。")
            if isinstance(node, exp.CTE) and not isinstance(node.this, query_types):
                raise ValueError("公共表表达式只允许 SELECT 查询。")

        return query.sql(dialect=dialect, pretty=True, comments=False,
                         unsupported_level=ErrorLevel.RAISE) + ";"
    except (SqlglotError, RecursionError):
        raise ValueError("无法按目标方言解析或生成这条只读 SELECT。") from None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connections", type=Path, default=DEFAULT_CONNECTIONS)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="列出连接名称、类型和默认数据库")
    commands.add_parser("match-mysql", help="从标准输入读取 MySQL 元数据并比对直连登记")
    validate = commands.add_parser("validate", help="从标准输入校验一条 SQL")
    validate.add_argument("--source-id", required=True)
    args = parser.parse_args()

    sys.stdin.reconfigure(encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8")
    try:
        connections = read_connections(args.connections)
        if args.command == "match-mysql":
            try:
                metadata = json.load(sys.stdin)
            except json.JSONDecodeError:
                raise ValueError("MySQL 连接元数据必须为有效 JSON。") from None
            print(json.dumps(match_mysql(connections, metadata), ensure_ascii=False))
            return 0

        sources = [describe_source(connection) for connection in connections]
        if args.command == "list":
            print(json.dumps({"sources": sources}, ensure_ascii=False))
            return 0

        matches = [source for source in sources if source["id"] == args.source_id]
        if len(matches) != 1:
            raise ValueError("请使用连接清单中唯一有效的连接编号。")
        source = matches[0]
        dialect = DIALECTS.get(source["engine"])
        if not dialect:
            raise ValueError("该连接未配置受支持的 SQL 方言。")

        sql = validate_select(sys.stdin.read(), dialect)
        print(json.dumps({"status": "accepted", "dialect": dialect, "sql": sql},
                         ensure_ascii=False))
        return 0
    except (OSError, ET.ParseError, UnicodeError):
        message = "无法读取连接清单或 SQL 输入，请检查文件路径与编码。"
    except ValueError as error:
        message = str(error)

    print(json.dumps({"status": "rejected", "message": message}, ensure_ascii=False))
    return 2


if __name__ == "__main__":
    sys.exit(main())
