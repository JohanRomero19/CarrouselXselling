import pandas as pd


def data_reading(path: str, lines_var: bool) -> pd.DataFrame:
    """
    read csv or json files
        Args:
            - path: string with the location of the file
            - lines_var:  bool with used if the json contain a json for line
        Returns:
            - dataframe readed
    """

    if "csv" in path:
        df = pd.read_csv(path)
    elif "json" in path:
        df = pd.read_json(path, lines=lines_var)
    else:
        raise ValueError("el archivo debe ser 'json' o 'csv'.")

    return df


def normalize_json(df: pd.DataFrame, column_name: str, separator: str) -> pd.DataFrame:
    """
    normilize the jsons in a column of a dataframe, and append it to the original dataframe
    Args:
        - df: df that contain json as column value
        - column_name: is a string that must contain the column name to normalize
        - separator: string with the separator of the values in the json
    Returns:
        - original dataframe with the original column deleted and appended the parsed data
    """

    dumped_json = df[column_name].map(lambda d: d if isinstance(d, dict) else {})

    event_df = pd.json_normalize(dumped_json, sep=separator)

    df = df.drop(columns=[column_name]).join(event_df)

    return df


def normalize_pays(pays_df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalization of columns and values in pays_df
    Args:
        - pays_df
    Returns:
        - normalized_pays_df
    """

    df = pays_df.copy()

    # colum pay_date validation and renaming to day

    if "pay_date" in df.columns:
        df["day"] = pd.to_datetime(df["pay_date"]).dt.date
        df = df.drop(columns=["pay_date"])
    elif "day" in df.columns:
        df["day"] = pd.to_datetime(df["day"]).dt.date
    else:
        raise ValueError("pays_df debe tener 'pay_date' o 'day'.")

    # validation of column value_prop exist in df

    if "value_prop" not in df.columns:
        # validate if the coluimn value_prop exist with another name
        for c in df.columns:
            if c.endswith("value_prop"):
                df = df.rename(columns={c: "value_prop"})
                break

    expected = {"day", "user_id", "value_prop", "total"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"pays_df faltan columnas: {missing}")

    return df[["day", "user_id", "value_prop", "total"]]


def normalize_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """
    normalize event dfs such prints_df and taps_df
    Args:
        - event df
    Returns:
        - normalized df
    """

    df = events_df.copy()

    # validate column day exist in df
    if "day" in df.columns:
        df["day"] = pd.to_datetime(df["day"]).dt.date
    else:
        raise ValueError("events_df debe tener columna 'day'.")

    # Secure renaming
    rename_map = {}
    # position
    if "position" not in df.columns:
        for c in df.columns:
            if c.endswith("position"):
                rename_map[c] = "position"
                break
    # value_prop
    if "value_prop" not in df.columns:
        for c in df.columns:
            if c.endswith("value_prop"):
                rename_map[c] = "value_prop"
                break

    if rename_map:
        df = df.rename(columns=rename_map)

    expected = {"day", "user_id", "position", "value_prop"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"events_df faltan columnas: {missing}")

    # Asegurar ints en position
    df["position"] = (
        pd.to_numeric(df["position"], errors="coerce").fillna(-1).astype(int)
    )

    return df[["day", "user_id", "value_prop", "position"]]


def aggregate_pays(pays_norm: pd.DataFrame) -> pd.DataFrame:
    """
    Groups all rows for diferent day, user and prop
    Args:
        - normalized pays df
    Returns:
        - pays df with total pays sum by diferent day, user and prop
    """

    agg = pays_norm.groupby(["day", "user_id", "value_prop"], as_index=False).agg(
        payments_total=("total", "sum"), payments_cnt=("total", "size")
    )
    return agg


def aggregate_events_wide(events_norm: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """
    Count events by (day, user_id, value_prop, position), pivot columns and sum totals to be 1
    Args:
        - normilized event df
        - prefix(prints or taps) to create pivot columns
    Returns:
        - agregated and pivoted df
    """

    long = (
        events_norm.groupby(
            ["day", "user_id", "value_prop", "position"], as_index=False
        )
        .size()
        .rename(columns={"size": f"{prefix}_cnt"})
    )
    wide = (
        long.pivot_table(
            index=["day", "user_id", "value_prop"],
            columns="position",
            values=f"{prefix}_cnt",
            aggfunc="sum",
            fill_value=0,
        )
        .rename(columns=lambda p: f"{prefix}_pos_{int(p)}")
        .reset_index()
    )

    for k in range(4):
        col = f"{prefix}_pos_{k}"
        if col not in wide.columns:
            wide[col] = 0

    wide[f"{prefix}_total"] = wide[[f"{prefix}_pos_{k}" for k in range(4)]].sum(axis=1)

    return wide


def funneling(
    pays_aggregated: pd.DataFrame,
    prints_aggregated: pd.DataFrame,
    taps_aggregated: pd.DataFrame,
    how,
) -> pd.DataFrame:
    """
    union of all dfs and validate respective columns
    Args:
        - pays_aggregated
        - prints_aggregated
        - taps_aggregated
    Returns:
        - merged df
    """

    try:
        merged_df = (
            pays_aggregated.merge(
                prints_aggregated, on=["day", "user_id", "value_prop"], how=how
            )
            .merge(taps_aggregated, on=["day", "user_id", "value_prop"], how=how)
            .fillna(0)
        )

        # convert to numeric columns
        num_cols = [
            c for c in merged_df.columns if c not in ["day", "user_id", "value_prop"]
        ]
        merged_df[num_cols] = (
            merged_df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        )
        # sort columns
        base = [
            "day",
            "user_id",
            "value_prop",
            "payments_total",
            "payments_cnt",
            "prints_total",
            "taps_total",
        ]
        pos_cols = [f"prints_pos_{k}" for k in range(4)] + [
            f"taps_pos_{k}" for k in range(4)
        ]
        cols = (
            [c for c in base if c in merged_df.columns]
            + [c for c in pos_cols if c in merged_df.columns]
            + [c for c in merged_df.columns if c not in base + pos_cols]
        )

        return merged_df.loc[:, cols]

    except (Exception, AttributeError, ValueError) as e:
        raise Exception(f"Error merging dfs: {e}")


def roll_sum_values(col: str, gb: pd.DataFrame, lookback_days: int, df: pd.DataFrame):

    s = gb.rolling(window=f"{lookback_days}D", on="day", closed="left")[col].sum()
    if len(s) != len(df):
        s = s.reset_index(level=[0, 1], drop=True)
    assert len(s) == len(df), f"Longitudes no coinciden: {len(s)} != {len(df)}"
    return s.fillna(0).to_numpy()


def build_prints_last_week_dataset(
    merged_df: pd.DataFrame,
    lookback_days: int = 21,
    last_week_days: int = 7,
    preaggregate_duplicates: bool = True,
) -> pd.DataFrame:
    """
    Build answers for questions
    Args:
        - funnel_df
        - lookback_days
        - last_week_days
        - preaggregate_duplicates
    Returns:
        - dataframe with prepared data
    """

    df = merged_df.copy()

    # securing types
    df["day"] = pd.to_datetime(df["day"], errors="coerce")
    if df["day"].isna().any():
        bad = df[df["day"].isna()]
        raise ValueError(f"Hay filas con 'day' inválido, ej: {bad.head(3)}")

    for col in ["prints_total", "taps_total", "payments_cnt", "payments_total"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        else:
            df[col] = 0

    # securing not duplicates column names
    if preaggregate_duplicates:
        df = df.groupby(["user_id", "value_prop", "day"], as_index=False).agg(
            {
                "prints_total": "sum",
                "taps_total": "sum",
                "payments_cnt": "sum",
                "payments_total": "sum",
                **{
                    c: "first"
                    for c in df.columns
                    if c
                    not in {
                        "user_id",
                        "value_prop",
                        "day",
                        "prints_total",
                        "taps_total",
                        "payments_cnt",
                        "payments_total",
                    }
                },
            }
        )

    # Sorting
    df = df.sort_values(["user_id", "value_prop", "day"]).reset_index(drop=True)

    gb = df.groupby(["user_id", "value_prop"], group_keys=False)

    # take data fron 3 las weeks
    df["prev3w_prints"] = roll_sum_values("prints_total", gb, lookback_days, df)
    df["prev3w_clicks"] = roll_sum_values("taps_total", gb, lookback_days, df)
    df["prev3w_payments_cnt"] = roll_sum_values("payments_cnt", gb, lookback_days, df)
    df["prev3w_payments_total"] = roll_sum_values(
        "payments_total", gb, lookback_days, df
    )

    # rename data and format
    df["prints_today"] = df["prints_total"]
    df["taps_today"] = df["taps_total"]
    df["payments_cnt_today"] = df["payments_cnt"]
    df["payments_total_today"] = df["payments_total"]
    df["clicked_today"] = (df["taps_today"] > 0).astype(int)

    # get last week
    max_day = df["day"].max()
    start_last_week = max_day - pd.Timedelta(days=last_week_days - 1)

    out = df.loc[
        (df["day"].between(start_last_week, max_day)) & (df["prints_today"] > 0),
        [
            "day",
            "user_id",
            "value_prop",
            "prints_today",
            "clicked_today",
            "prev3w_prints",
            "prev3w_clicks",
            "prev3w_payments_cnt",
            "prev3w_payments_total",
            "payments_cnt_today",
            "payments_total_today",
            "taps_today",
        ],
    ].reset_index(drop=True)

    return out
