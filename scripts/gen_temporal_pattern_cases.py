#!/usr/bin/env python3
"""Write pkg/cypher/testdata/temporal_patterns_neo4j.json: Neo4j 2026.x's answers for format()
and the temporal constructors' pattern form, which TestTemporalPatternsMatchNeo4j replays.

    pip install neo4j
    python3 scripts/gen_temporal_pattern_cases.py bolt://localhost:7687 > pkg/cypher/testdata/temporal_patterns_neo4j.json
"""
import json
import string
import sys
from neo4j import GraphDatabase

VALUES = {
    "DT": "datetime('1986-11-08T06:04:05.123456789+01:00[Europe/Berlin]')",
    "DTS": "datetime('1986-07-08T21:04:05+02:00[Europe/Berlin]')",
    "DTO": "datetime('2021-03-04T22:04:05.5-05:30')",
    "DTZ": "datetime('2021-03-04T12:00Z')",
    "DTNY": "datetime('2021-07-04T12:00[America/New_York]')",
    "LDT": "localdatetime('1986-11-08T16:04:05.0123')",
    "D": "date('1986-11-08')",
    "D2": "date('-0044-03-15')",
    "D3": "date('+12021-01-01')",
    "T": "time('06:04:05.5+05:30')",
    "TZ": "time('12:00Z')",
    "LT": "localtime('16:04:05')",
    "LT0": "localtime('00:00:00')",
    "LTN": "localtime('12:00:00')",
    "DU": "duration('P1Y2M3DT4H5M6.007S')",
    "DU2": "duration('-P1Y2M3DT4H5M6.007S')",
    "DU3": "duration('P14DT49H130M70.123456789S')",
    "DU4": "duration('PT0.5S')",
    "TS": "time('06:04:05+02:30:15')",
    "LTE": "localtime('19:30')",
}
FORMAT_PATTERNS = [c * n for c in string.ascii_letters for n in range(1, 6)] + [
    "yyyy-MM-dd", "yy/M/d", "HH:mm:ss", "h:mm a", "dd.MM.yyyy HH:mm", "'Quoted' yyyy", "''", "'it''s' yyyy", "yyyy 'T",
    "yyyy-MM-dd'T'HH:mm", "#", "{", "}", "yyyy#", "-/,.:; ", "ü", "[yyyy][HH]", "[[yyyy]]", "yyyy]", "[yyyy", "p", "pp",
    "ppd", "ppyyyy", "pppppMM", "''''", "", "  ", "y M", "M d", "d H", "H m", "m s", "s S", "s SSS", "s n", "S n", "SSS n",
    "SSSSSSSSS", "w d", "W d", "y w", "D H", "M D", "u M", "y u", "H:m:s", "HH:mm:ss.SSSSSS", "d'd' H'h'", "Q M", "y Q",
    "y Q M", "H s", "s A", "m A", "H N", "Q d", "w y", "SSSSSSSSS n", "yyyy-MM-dd HH:mm:ss.SSS XXX '['VV']'", "EEEE, MMMM d, yyyy", "YYYY-'W'ww-e",
    "uuuu-DDD", "g", "zzzz '('z')'", "vvvv (v)", "OOOO", "xxx", "B", "BBBB", "BBBBB", "hh 'o''clock' a",
]
WEEK_DATES = [f"{y}-{m:02d}-{d:02d}" for y in (2015, 2016, 2020, 2021, 2022, 2023, 2026, 2027) for m, d in
              ((12, 25), (12, 28), (12, 31), (1, 1), (1, 2), (1, 3), (1, 7), (1, 8))]
PARSE = [
    ("date", "18.11.1986", "dd.MM.yyyy"), ("date", "18.11.1986", "d.M.y"), ("date", "8.1.1986", "d.M.yyyy"),
    ("date", "08.01.86", "dd.MM.yy"), ("date", "08.01.1986", "dd.MM.yy"), ("date", "19861118", "yyyyMMdd"),
    ("date", "1986111", "yyyyMMd"), ("date", "861118", "yyMMdd"), ("date", "1986-11-18", "uuuu-MM-dd"),
    ("date", "nov 18 1986", "MMM dd yyyy"), ("date", "Nov 18 1986", "MMM dd yyyy"), ("date", "November 18 1986", "MMMM dd yyyy"),
    ("date", "Tue 18 Nov 1986", "EEE dd MMM yyyy"), ("date", "Mon 18 Nov 1986", "EEE dd MMM yyyy"),
    ("date", "Tuesday, November 18, 1986", "EEEE, MMMM d, yyyy"), ("date", "1986-322", "yyyy-DDD"), ("date", "1986-W47-2", "YYYY-'W'ww-e"),
    ("date", "1986-W47-Mon", "YYYY-'W'ww-EEE"), ("date", "1986 Q4", "yyyy QQQ"), ("date", "2021-02-29", "yyyy-MM-dd"),
    ("date", "2021-04-31", "yyyy-MM-dd"), ("date", "2021-13-01", "yyyy-MM-dd"), ("date", "2021-02-32", "yyyy-MM-dd"),
    ("date", " 2021-02-01", "yyyy-MM-dd"), ("date", "2021-02-01 ", "yyyy-MM-dd"), ("date", "2021-2-1", "yyyy-MM-dd"),
    ("date", "2021-02-01", "yyyy-M-d"), ("date", "+2021-02-01", "yyyy-MM-dd"), ("date", "12021-02-01", "yyyy-MM-dd"),
    ("date", "+12021-02-01", "yyyy-MM-dd"), ("date", "2021-02-01", "yyyy-MM-dd[ HH:mm]"), ("date", "2021-02-01 10:30", "yyyy-MM-dd[ HH:mm]"),
    ("date", "2021-02-01T10:30", "yyyy-MM-dd'T'HH:mm"), ("date", "2021-02-01T10:30+01:00", "yyyy-MM-dd'T'HH:mmXXX"),
    ("date", "1 AD 2021-02-01", "G yyyy-MM-dd"), ("date", "AD 2021-02-01", "G yyyy-MM-dd"), ("date", "2021-02-01", "{"),
    ("date", "2021-02-01", ""), ("date", "", ""), ("date", "2021-02-01", "yyyy-MM-dd'"), ("date", "2021", "yyyy[-MM[-dd]]"),
    ("date", "2021-02-01 24:00", "yyyy-MM-dd HH:mm"), ("date", "59000", "g"), ("date", "2021-02 1 Mon", "yyyy-MM W EEE"),
    ("date", "2021-02 2 Mon", "yyyy-MM F EEE"), ("date", "2021-366", "yyyy-DDD"), ("date", "Q1 2021-02-01", "QQQ yyyy-MM-dd"),
    ("date", "Q2 2021-02-01", "QQQ yyyy-MM-dd"), ("date", "  8.11.1986", "ppd.MM.yyyy"), ("date", "18.11.1986", "ppd.MM.yyyy"),
    ("date", "2021-02-01 25:00", "yyyy-MM-dd HH:mm"),
    ("localtime", "13", "HH"), ("localtime", "1 PM", "h a"), ("localtime", "1 pm", "h a"), ("localtime", "13:45", "hh:mm"),
    ("localtime", "1:45", "h:mm"), ("localtime", "24:00", "HH:mm"), ("localtime", "24:00", "kk:mm"), ("localtime", "24:30", "HH:mm"),
    ("localtime", "13:45:30.123456789", "HH:mm:ss.SSSSSSSSS"), ("localtime", "13:45:30.1234", "HH:mm:ss.SSS"),
    ("localtime", "13:45:30.12", "HH:mm:ss.SSS"), ("localtime", "13:45:30.5", "HH:mm:ss.S"), ("localtime", "13:45:30 123", "HH:mm:ss n"),
    ("localtime", "49530000", "A"), ("localtime", "45", "mm"), ("localtime", "1986-11-18 13:45", "yyyy-MM-dd HH:mm"),
    ("localtime", "13:45+02:00", "HH:mmXXX"), ("localtime", "in the afternoon 1", "B h"), ("localtime", "at night 10", "B h"),
    ("localtime", "midnight 12", "B h"), ("localtime", "noon 12", "B h"), ("localtime", "in the morning 3", "B h"),
    ("localtime", "13 PM", "H a"), ("localtime", "13 AM", "H a"), ("localtime", "134530", "HHmmss"), ("localtime", "1345301", "HHmmssS"),
    ("localtime", "13:45 30", "HH:mm s"), ("localtime", "13 30", "HH s"), ("localtime", "0 AM", "K a"), ("localtime", "11 PM", "K a"),
    ("localtime", "12 AM", "K a"), ("localtime", "13:45 in the afternoon", "HH:mm B"), ("localtime", "13:45 in the morning", "HH:mm B"),
    ("time", "13:45", "HH:mm"), ("time", "13:45+02:00", "HH:mmXXX"), ("time", "13:45 Europe/Berlin", "HH:mm VV"),
    ("time", "13:45 +0200", "HH:mm Z"), ("time", "13:45 Z", "HH:mm X"), ("time", "13:45 GMT+2", "HH:mm O"),
    ("time", "13:45 GMT+02:00", "HH:mm OOOO"), ("time", "13:45 GMT", "HH:mm O"), ("time", "13:45 +02", "HH:mm X"),
    ("time", "13:45 +0230", "HH:mm X"), ("time", "13:45 +02:30", "HH:mm X"), ("time", "13:45 +00", "HH:mm x"),
    ("time", "13:45 Z", "HH:mm x"), ("time", "13:45 +0000", "HH:mm Z"), ("time", "13:45 -05:30", "HH:mm ZZZZZ"),
    ("time", "13:45 GMT-05:30", "HH:mm ZZZZ"), ("time", "13:45 +02:30:15", "HH:mm XXXXX"),
    ("localdatetime", "1986 11 18 06 04", "yyyy MM dd HH mm"), ("localdatetime", "1986-11-18", "yyyy-MM-dd"),
    ("localdatetime", "06:04", "HH:mm"), ("localdatetime", "1986-11-18 06:04+01:00", "yyyy-MM-dd HH:mmXXX"),
    ("localdatetime", "1986-11-18 24:00", "yyyy-MM-dd HH:mm"), ("localdatetime", "1986-12-31 24:00", "yyyy-MM-dd kk:mm"),
    ("datetime", "1986-11-18 06:04 +0100", "yyyy-MM-dd HH:mm Z"), ("datetime", "1986-11-18 06:04 Europe/Berlin", "yyyy-MM-dd HH:mm VV"),
    ("datetime", "1986-07-18 06:04 Europe/Berlin", "yyyy-MM-dd HH:mm VV"), ("datetime", "1986-11-18 06:04 CET", "yyyy-MM-dd HH:mm z"),
    ("datetime", "1986-11-18 06:04 PST", "yyyy-MM-dd HH:mm z"), ("datetime", "1986-11-18 06:04 GMT+2", "yyyy-MM-dd HH:mm O"),
    ("datetime", "1986-11-18 06:04 Z", "yyyy-MM-dd HH:mm X"), ("datetime", "1986-11-18 06:04 +01", "yyyy-MM-dd HH:mm X"),
    ("datetime", "1986-11-18 06:04:05.5 +01:00", "yyyy-MM-dd HH:mm:ss.S xxx"), ("datetime", "1986-11-18", "yyyy-MM-dd"),
    ("datetime", "1986-11-18 06:04", "yyyy-MM-dd HH:mm"), ("datetime", "1986-11-18 25:00", "yyyy-MM-dd HH:mm"),
    ("datetime", "1986-11-18 06:04 Mars/Phobos", "yyyy-MM-dd HH:mm VV"),
    ("datetime", "1986-11-18 06:04 +01:00 Europe/Berlin", "yyyy-MM-dd HH:mm XXX VV"),
    ("datetime", "1986-11-18 06:04 Central European Standard Time", "yyyy-MM-dd HH:mm zzzz"),
    ("datetime", "1986-11-18 06:04 Pacific Time", "yyyy-MM-dd HH:mm vvvv"), ("datetime", "1986-11-18 06:04 PT", "yyyy-MM-dd HH:mm v"),
    ("datetime", "1986-11-18 06:04 Asia/Tokyo", "yyyy-MM-dd HH:mm z"), ("datetime", "1986-11-18 06:04 UTC", "yyyy-MM-dd HH:mm VV"),
    ("datetime", "1986-11-18 06:04 Etc/GMT+5", "yyyy-MM-dd HH:mm VV"), ("datetime", "1986-11-18 06:04 -03:00", "yyyy-MM-dd HH:mm VV"),
    ("datetime", "2021-10-31 02:30 +02:00 Europe/Berlin", "yyyy-MM-dd HH:mm XXX VV"),
    ("datetime", "2021-10-31 02:30 +01:00 Europe/Berlin", "yyyy-MM-dd HH:mm XXX VV"),
    ("datetime", "2021-03-28 02:30 Europe/Berlin", "yyyy-MM-dd HH:mm VV"),
    ("datetime", "1986-11-18 06:04 EST", "yyyy-MM-dd HH:mm z"), ("datetime", "1986-11-18 06:04 IST", "yyyy-MM-dd HH:mm z"),
    # Rejected and boundary forms of each field, offset and resolution rule.
    ("localtime", "49530000000000", "N"), ("date", "2020 2021", "yyyy yyyy"), ("date", "8", "ppd"),
    ("date", "4th quarter 2021-11-01", "QQQQ yyyy-MM-dd"), ("date", "4 2021-11-01", "QQQQQ yyyy-MM-dd"),
    ("date", "4 2021-11-01", "Q yyyy-MM-dd"), ("date", "3 2021-11-01", "Q yyyy-MM-dd"), ("localtime", "mi 12", "BBBBB h"),
    ("localtime", "n 12", "BBBBB h"), ("time", "13:45 XYZ", "HH:mm O"), ("time", "13:45 GMT+x", "HH:mm O"),
    ("time", "13:45 GMT+19", "HH:mm O"), ("time", "13:45 GMT+1:3", "HH:mm O"), ("time", "13:45 GMT+1:30:15", "HH:mm O"),
    ("time", "13:45 XYZ", "HH:mm ZZZZ"), ("time", "13:45 GMT+1x", "HH:mm ZZZZ"),
    ("time", "13:45 +01:00 +02:00", "HH:mm XXX xxx"), ("time", "13:45 +01:00 +01:00", "HH:mm XXX xxx"),
    ("datetime", "2020-01-01 10:00 UTC+01:00", "yyyy-MM-dd HH:mm VV"), ("datetime", "2020-01-01 10:00 GMT-05:30", "yyyy-MM-dd HH:mm VV"),
    ("datetime", "2020-01-01 10:00 UTC+", "yyyy-MM-dd HH:mm VV"), ("datetime", "2020-01-01 10:00 Z", "yyyy-MM-dd HH:mm VV"),
    ("datetime", "2020-01-01 10:00 Europe/Berlin Europe/Paris", "yyyy-MM-dd HH:mm VV VV"),
    ("datetime", "2020-01-01 10:00 Europe/Berlin Europe/Berlin", "yyyy-MM-dd HH:mm VV VV"),
    ("date", "+5.11.2020", "d.MM.yyyy"), ("date", "-5.11.2020", "dd.MM.yyyy"), ("date", "-5.11.2020", "d.MM.yyyy"),
    ("date", "-0.11.2020", "d.MM.yyyy"), ("date", "202011", "yyyyMMdd"), ("localtime", "12345678901234567890", "n"),
    ("time", "13:45 +", "HH:mm VV"), ("time", "13:45 +1", "HH:mm VV"), ("time", "13:45 +19:00", "HH:mm VV"),
    ("time", "13:45 +01:00", "HH:mm VV"), ("time", "13:45 0100", "HH:mm XX"), ("time", "13:45 +1900", "HH:mm XX"),
    ("time", "13:45 +01", "HH:mm XX"), ("time", "13:45 +01:0", "HH:mm XXX"), ("time", "13:45 +01:60", "HH:mm XXX"),
    ("date", "AD 0-01-01", "G y-MM-dd"), ("date", "BC 44-03-15", "G y-MM-dd"), ("date", "2020 2021-01-01", "u y-MM-dd"),
    ("date", "2020 2020-01-01", "u y-MM-dd"), ("date", "2021-02 6 Mon", "yyyy-MM F EEE"), ("date", "2021-13 1 Mon", "yyyy-MM F EEE"),
    ("date", "2021-02 5 Mon", "yyyy-MM F EEE"), ("date", "2021-60-1", "YYYY-ww-e"), ("date", "2021-01-9", "YYYY-ww-e"),
    ("date", "2021-02 5 Mon", "yyyy-MM W EEE"), ("date", "2021-13 1 Mon", "yyyy-MM W EEE"), ("date", "2021-02 1 Sun", "yyyy-MM W EEE"),
    ("localtime", "0:00", "k:mm"), ("localtime", "25:00", "k:mm"), ("localtime", "13 13", "k H"), ("localtime", "13 14", "k H"),
    ("localtime", "1 1 AM", "h K a"), ("localtime", "1 2 AM", "h K a"), ("localtime", "13 AM", "h a"), ("localtime", "12 PM", "K a"),
    ("localtime", "13:61", "HH:mm"), ("localtime", "86400000", "A"), ("localtime", "13:45 5", "HH:mm n"),
    ("localtime", "13 1", "HH h"), ("localtime", "13 2", "HH h"), ("localtime", "13:00 noon", "HH:mm B"),
    ("localtime", "12:00 noon", "HH:mm B"), ("localtime", "13:00:00.5 49530000", "HH:mm:ss.S A"),
    ("localtime", "49530500 5", "A S"), ("date", "0000-01-01 BC", "uuuu-MM-dd G"), ("date", "0000-01-01 AD", "uuuu-MM-dd G"),
    ("date", "2020-01-01 Mon", "yyyy-MM-dd EEE"), ("date", "2020-01-01 Wed", "yyyy-MM-dd EEE"), ("date", "2020-01-01 4", "yyyy-MM-dd e"),
    ("date", "2020-01-01 5", "yyyy-MM-dd e"), ("date", "2020-01-01 58849", "yyyy-MM-dd g"), ("date", "2020-01-01 1", "yyyy-MM-dd F"),
    ("date", "2020-01-01 2", "yyyy-MM-dd F"), ("date", "2020-01-01 2020-01-1", "yyyy-MM-dd YYYY-ww-e"),
    ("localtime", "1 PM 13", "h a H"), ("localtime", "1 PM 12", "h a H"), ("localtime", "13:45:30.5 5", "HH:mm:ss.S n"),
    ("date", "20", "yyyyMM"), ("date", "20ab", "yyyyMM"), ("time", "13:45 +x", "HH:mm X"), ("time", "13:45 +01", "HH:mm XXX"),
    ("date", "2021-02 6 Mon", "yyyy-MM W EEE"), ("localtime", "at night 3", "B h"),
    ("date", "2021-02 7 Mon", "yyyy-MM W EEE"), ("date", "2021-02 0 Mon", "yyyy-MM W EEE"), ("localtime", "03:00 at night", "HH:mm B"),
    ("localtime", "at night 7", "B h"),
]


def run(s, query, params):
    try:
        rows = [r.values() for r in s.run("CYPHER 25 " + query, params)]
        return "=" + rows[0][0] if rows[0][0] is not None else "null"
    except Exception as e:
        code = getattr(e, "code", "") or type(e).__name__
        msg = str(getattr(e, "message", e)).split("\n")[0]
        return "!" + code + ": " + msg


driver = GraphDatabase.driver(sys.argv[1] if len(sys.argv) > 1 else "bolt://localhost:7687", auth=None)
out = {"values": VALUES, "format": [], "parse": []}
with driver.session() as s:
    for key, value in VALUES.items():
        for pattern in FORMAT_PATTERNS:
            result = run(s, f"RETURN format({value}, $p) AS v", {"p": pattern})
            out["format"].append([key, pattern, result.split(":")[0] if result.startswith("!") else result])
        out["format"].append([key, None, run(s, f"RETURN format({value}) AS v", {})])
    for kind, text, pattern in PARSE:
        out["parse"].append([kind, text, pattern, run(s, f"RETURN toString({kind}($t, $p)) AS v", {"t": text, "p": pattern})])
    for date in WEEK_DATES:
        printed = run(s, "RETURN format(date($d), 'YYYY-ww-e W F') AS v", {"d": date})
        out["format"].append([date, "YYYY-ww-e W F", printed])
        out["values"][date] = f"date('{date}')"
        if printed.startswith("="):
            year_week = printed[1:].rsplit(" ", 2)[0]
            out["parse"].append(["date", year_week, "YYYY-ww-e", run(s, "RETURN toString(date($t, $p)) AS v", {"t": year_week, "p": "YYYY-ww-e"})])
driver.close()
print("{")
print('"values": ' + json.dumps(out["values"], ensure_ascii=False) + ",")
for key in ("format", "parse"):
    print(f'"{key}": [')
    print(",\n".join(json.dumps(row, ensure_ascii=False) for row in out[key]))
    print("]" + ("," if key == "format" else ""))
print("}")
