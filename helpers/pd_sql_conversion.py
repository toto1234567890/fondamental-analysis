#!/usr/bin/env python
# coding:utf-8

from decimal import Decimal
from typing import List, Optional, Any, Tuple
from re import sub as reSub, match as reMatch
import logging

import pandas as pd
import numpy as np

from psycopg2.extras import RealDictCursor


def _normalize_number_string(s: str) -> str:
    """Remove currency symbols/whitespace, normalize European 1.234,56 -> 1234.56
       Return original-ish string that pd.to_numeric can parse, or empty -> fail.
    """
    if pd.isna(s):
        return ""
    s = str(s).strip()
    if s == "":
        return ""
    # remove currency symbols and letters except digits, ., ,, -, e, E
    s = reSub(r"[^\d\-\.,eE%+]", "", s)
    # handle percentages: keep % for later handling
    percent = s.endswith('%')
    if percent:
        s = s[:-1]

    # Heuristics to normalize:
    # If both '.' and ',' exist: assume '.' thousand, ',' decimal if comma occurs rightmost
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")

    # If only commas and many commas -> remove commas (thousands)
    elif "," in s and s.count(",") > 0:
        # If pattern like 1.234.567,89 (no dot), we already handled above; here only commas:
        # if comma used as decimal (e.g., "1234,56") -> replace comma with dot if there are exactly one comma and it's near end
        if s.count(",") == 1 and reMatch(r"^-?\d+,\d+$", s):
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")

    # Remove leading plus signs
    s = s.lstrip('+')

    # Keep exponential notation as-is if present
    return ("-" + s) if s == "-" else s


def _parse_numeric_value(val: Any) -> float:
    if pd.isna(val):
        return np.nan
    s = _normalize_number_string(val)
    if s == "":
        return np.nan
    try:
        return float(s)
    except Exception:
        # last attempt: Decimal parse then to float (safer for big precision)
        try:
            return float(Decimal(s))
        except Exception:
            return np.nan


def _parse_integer_series(s: pd.Series) -> Tuple[pd.Series, List[int]]:
    """Return integer Series (nullable Int64) and list of failing row indices."""
    numeric = s.apply(_parse_numeric_value)
    # Identify non-null rows that are non-integer (within tolerance)
    notnull_idx = numeric.notna()
    # If numeric value is integer-like
    frac = (numeric[notnull_idx] - np.round(numeric[notnull_idx])).abs()
    fails = list(frac[frac > 1e-9].index)  # indexes where numeric is not integer
    # Cast integer-like to pandas nullable Int64
    out = pd.Series(pd.NA, index=s.index, dtype="Int64")
    ok_idx = numeric.notna() & ~numeric.isna()
    int_like_idx = ok_idx & (numeric.round().isnull() == False) & (numeric == np.round(numeric))
    # convert round to int
    out.loc[int_like_idx] = numeric.loc[int_like_idx].round().astype("Int64")
    return out, fails


def _parse_numeric_series(s: pd.Series) -> Tuple[pd.Series, List[int]]:
    numeric = s.apply(_parse_numeric_value)
    fails = list(numeric[numeric.isna() & s.notna()].index)
    # keep floats as float64; NaN for failures
    return numeric.astype("float64"), fails


def _parse_datetime_series(s: pd.Series) -> Tuple[pd.Series, List[int]]:
    parsed = pd.to_datetime(s, errors="coerce", infer_datetime_format=True, utc=False)
    fails = list(parsed[parsed.isna() & s.notna()].index)
    return parsed, fails


def _parse_boolean_series(s: pd.Series) -> Tuple[pd.Series, List[int]]:
    map_true = {"true", "t", "yes", "y", "1", "on"}
    map_false = {"false", "f", "no", "n", "0", "off"}
    out = pd.Series(pd.NA, index=s.index, dtype="boolean")
    fails = []
    for i, v in s.items():
        if pd.isna(v):
            out.iat[i] = pd.NA
            continue
        st = str(v).strip().lower()
        if st in map_true:
            out.iat[i] = True
        elif st in map_false:
            out.iat[i] = False
        else:
            fails.append(i)
            out.iat[i] = pd.NA
    return out, fails


def _get_postgresql_table_schema(conn, table_name: str, schema: str = "public") -> dict:
    """Get PostgreSQL table column types using psycopg2."""
    query = """
    SELECT column_name, data_type, udt_name
    FROM information_schema.columns 
    WHERE table_name = %s AND table_schema = %s
    ORDER BY ordinal_position;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (table_name, schema))
        results = cur.fetchall()
    
    schema_info = {}
    for row in results:
        schema_info[row['column_name']] = {
            'data_type': row['data_type'],
            'udt_name': row['udt_name']
        }
    return schema_info


def auto_cast_to_sql(conn, table_name: str, df: pd.DataFrame,
                            schema: str = "public",
                            logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Deterministically cast DataFrame columns to match PostgreSQL table column types.
    - conn: psycopg2 connection
    - table_name: target table name
    - df: DataFrame (will copy internally)
    - schema: PostgreSQL schema name (default: 'public')
    - logger: Logger instance for warnings (if None, uses print)
    Returns: converted_df
    Always continues with warnings, never raises errors.
    """
    # Set up logger if not provided
    if logger is None:
        logger = logging.getLogger(__name__)
    
    # Get table schema from PostgreSQL
    try:
        table_schema = _get_postgresql_table_schema(conn, table_name, schema)
        if not table_schema:
            logger.warning(f"Table {schema}.{table_name} not found in database or has no columns")
            return df
    except Exception as e:
        logger.warning(f"Failed to get schema for table {schema}.{table_name}: {e}")
        return df

    out_df = df.copy()
    total_fails = 0

    for colname, colinfo in table_schema.items():
        if colname not in out_df.columns:
            logger.warning(f"Column '{colname}' not found in DataFrame")
            continue

        series = out_df[colname]
        data_type = colinfo['data_type']
        udt_name = colinfo['udt_name']

        # Integer types
        if data_type in ('integer', 'bigint', 'smallint', 'serial', 'bigserial'):
            parsed, fails = _parse_integer_series(series)
            if fails:
                logger.warning(
                    f"Column '{colname}': {len(fails)} values failed integer conversion "
                    f"(SQL type: {data_type}). Failed indices sample: {fails[:10]}"
                )
                total_fails += len(fails)
            out_df[colname] = parsed

        # Numeric/decimal types
        elif data_type in ('numeric', 'decimal', 'real', 'double precision'):
            parsed, fails = _parse_numeric_series(series)
            if fails:
                logger.warning(
                    f"Column '{colname}': {len(fails)} values failed numeric conversion "
                    f"(SQL type: {data_type}). Failed indices sample: {fails[:10]}"
                )
                total_fails += len(fails)
            out_df[colname] = parsed

        # Date/time types
        elif data_type in ('timestamp without time zone', 'timestamp with time zone', 
                          'date', 'time without time zone', 'time with time zone'):
            parsed, fails = _parse_datetime_series(series)
            if fails:
                logger.warning(
                    f"Column '{colname}': {len(fails)} values failed datetime conversion "
                    f"(SQL type: {data_type}). Failed indices sample: {fails[:10]}"
                )
                total_fails += len(fails)

            # If SQL type is date, convert to date objects
            if data_type == 'date':
                out_df[colname] = parsed.dt.date.where(parsed.notna(), other=pd.NaT)
            else:
                out_df[colname] = parsed

        # Boolean
        elif data_type == 'boolean':
            parsed, fails = _parse_boolean_series(series)
            if fails:
                logger.warning(
                    f"Column '{colname}': {len(fails)} values failed boolean conversion "
                    f"(SQL type: {data_type}). Failed indices sample: {fails[:10]}"
                )
                total_fails += len(fails)
            out_df[colname] = parsed

        # Text types
        elif data_type in ('text', 'character varying', 'character', 'varchar', 'char'):
            # keep as string, but ensure NaN for empty-like
            out_df[colname] = series.where(series.notna(), other=None).astype(object)

        # JSON types
        elif data_type in ('json', 'jsonb'):
            # For JSON, we try to keep as-is or convert to dict if string
            def _to_json_like(v):
                if pd.isna(v):
                    return None
                if isinstance(v, (dict, list)):
                    return v
                try:
                    import json
                    return json.loads(v)
                except:
                    return v
            converted = series.apply(_to_json_like)
            out_df[colname] = converted

        # Binary types
        elif data_type in ('bytea',) or udt_name == 'bytea':
            def _to_bytes(v):
                if pd.isna(v):
                    return None
                if isinstance(v, (bytes, bytearray)):
                    return bytes(v)
                try:
                    return bytes(v, "utf-8")
                except Exception:
                    return None
            converted = series.apply(_to_bytes)
            fails = list(converted[converted.isnull() & series.notna()].index)
            if fails:
                logger.warning(
                    f"Column '{colname}': {len(fails)} values failed binary conversion "
                    f"(SQL type: {data_type}). Failed indices sample: {fails[:10]}"
                )
                total_fails += len(fails)
            out_df[colname] = converted

        else:
            # Unknown type: cast to string
            out_df[colname] = series.astype(object)
            logger.warning(f"Column '{colname}': Unknown SQL type {data_type}, cast to string")

    # Log summary
    if total_fails > 0:
        logger.warning(f"Table '{schema}.{table_name}': Total {total_fails} conversion failures across all columns")
    else:
        logger.info(f"Table '{schema}.{table_name}': All conversions successful")

    return out_df