import pandas as pd
import warnings

warnings.filterwarnings(
    "ignore",
    message=r"Could not infer format, so each element will be parsed individually",
    category=UserWarning,
)
warnings.filterwarnings(
    "ignore",
    message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated",
    category=FutureWarning,
)


# id, name, date only values in all 3 datasets so strategy is to append into single table
def transform_clients(data_frames):
    cleaned = []

    for data_frame in data_frames:
        df = data_frame.copy()

        for col in df.columns:
            # clean id's of all rows (clean spaces, capitalize)
            s = df[col].astype(str).str.strip()

            # Verify there is a letter starting in the string and the length is 5 and then drop invalid rows
            valid_ids = s.str.match(r"^[A-Z][0-9]{5}$", na=False)
            # if 80% of rows match pattern we rename the column to client_id and drop bad rows
            if valid_ids.mean() > 0.8 and "client_id" not in df.columns:
                df = df.rename(columns={col: "client_id"})
                # drop rows that don't match id regex
                df = df[valid_ids].reset_index(drop=True)

            # Verify there is a space in the middle and letters on both sides and then drop invalid rows
            valid_names = s.str.match(r"^[A-Za-z'-]+ [A-Za-z'-]+$", na=False)
            # if 80% of rows match we rename the column to client_name and drop bad rows
            if valid_names.mean() > 0.8:
                df = df.rename(columns={col: "client_name"})
                df = df[valid_names].reset_index(drop=True)
                # make lower, clean spaces, title mode, replace non-alphabetic characters excluding spaces
                df["client_name"] = s.str.strip().str.lower().str.title().str.replace(r'[^A-Za-z ]', '', regex=True)

            st = s.str.lower()

            # Verify that rows contain status
            valid_status = st.isin(['active', 'inactive', 'y', 'n'])
            # if 80% of rows match we rename the column to client_status
            if valid_status.mean() > 0.8:
                df = df.rename(columns={col: "status"})
                st = df["status"].astype(str).str.strip().str.lower()
                df.loc[~st.isin(['active', 'inactive', 'y', 'n']), 'status'] = pd.NA
                df["status"] = st.map({
                    'active': 'ACTIVE',
                    'inactive': 'INACTIVE',
                    'y': 'ACTIVE',
                    'n': 'INACTIVE'
                })
                # treat blanks/unknowns as NA
                df.loc[~st.isin(['active', 'inactive', 'y', 'n']), 'status'] = pd.NA

            # Try to parse dates in many formats (found online)
            parsed = pd.to_datetime(s, errors="coerce")
            mask = parsed.isna()
            if mask.any():
                fmts = [
                    "%m/%d/%Y", "%m/%d/%y",
                    "%Y/%m/%d %H:%M:%S", "%Y/%m/%d",
                    "%d-%b-%Y", "%d-%b-%y",
                    "%b %d, %Y",
                    "%d-%m-%Y",
                ]
                for fmt in fmts:
                    t = pd.to_datetime(s[mask], format=fmt, errors="coerce")
                    parsed.loc[mask & t.notna()] = t
                    mask = parsed.isna()
                    if not mask.any():
                        break

            valid_dates = parsed.notna()

            # if 80% of rows match we rename the column to start_date
            if valid_dates.mean() > 0.4:
                df = df.rename(columns={col: "start_date"})
                ser = parsed.copy()

                # if tz-aware, make tz-naive first (only if tz is present)
                if getattr(ser.dt, "tz", None) is not None:
                    ser = ser.dt.tz_convert(None)

                # normalize to date-only and format as YYYY-MM-DD
                df["start_date"] = ser.dt.normalize().dt.strftime("%Y-%m-%d")

            # Search for currency columns and remove
            currency = st.isin(['usd'])
            if currency.mean() > 0.8:
                df.drop(columns=col, inplace=True)

        # force existing of the 5 essential columns
        columns_required = ['client_id', 'client_name', 'status', 'start_date', 'tier']
        for c in columns_required:
            if c not in df.columns:
                df[c] = df[c] = pd.NA
        df = df[columns_required]
        cleaned.append(df)
    return cleaned


def transform_invoices(data_frames):
    cleaned = []

    for data_frame in data_frames:
        df = data_frame.copy()

        for col in df.columns:
            # clean id's of all rows (clean spaces, capitalize)
            s = df[col].astype(str).str.strip()

            # Verify there is INV-(str) starting in the string and the length is 7 and then drop invalid rows
            valid_inv_ids = s.str.fullmatch(r"INV-[A-Z0-9]{7}", na=False)
            # if 80% of rows match pattern we rename the column to invoice_id and drop bad rows
            if valid_inv_ids.mean() > 0.8 and "invoice_id" not in df.columns:
                df = df.rename(columns={col: "invoice_id"})
                # drop rows that don't match id regex
                df = df[valid_inv_ids].reset_index(drop=True)

            # Verify there is a letter starting in the string and the length is 5 and then drop invalid rows
            valid_ids = s.str.match(r"^[A-Z][0-9]{5}$", na=False)
            # if 80% of rows match pattern we rename the column to client_id and drop bad rows
            if valid_ids.mean() > 0.8 and "client_id" not in df.columns:
                df = df.rename(columns={col: "client_id"})
                # drop rows that don't match id regex
                df = df[valid_ids].reset_index(drop=True)

            # Search for currency columns and remove
            currency = s.isin(['USD'])
            if currency.mean() > 0.8:
                df.drop(columns=col, inplace=True)

            # Verify there is a space in the middle and letters on both sides and then drop invalid rows
            valid_names = s.str.match(r"^[A-Za-z'-]+ [A-Za-z'-]+$", na=False)
            # if 80% of rows match we rename the column to client_name and drop bad rows
            if valid_names.mean() > 0.8:
                df = df.rename(columns={col: "client_name"})
                df = df[valid_names].reset_index(drop=True)
                # make lower, clean spaces, title mode, replace non-alphabetic characters excluding spaces
                df["client_name"] = s.str.strip().str.lower().str.title().str.replace(r'[^A-Za-z ]', '', regex=True)

            # Try to parse dates in many formats (found online)
            parsed = pd.to_datetime(s, errors="coerce")
            mask = parsed.isna()
            if mask.any():
                fmts = [
                    "%m/%d/%Y", "%m/%d/%y",
                    "%Y/%m/%d %H:%M:%S", "%Y/%m/%d",
                    "%d-%b-%Y", "%d-%b-%y",
                    "%b %d, %Y",
                    "%d-%m-%Y",
                ]
                for fmt in fmts:
                    t = pd.to_datetime(s[mask], format=fmt, errors="coerce")
                    parsed.loc[mask & t.notna()] = t
                    mask = parsed.isna()
                    if not mask.any():
                        break

            valid_dates = parsed.notna()

            # if 80% of rows match we rename the column to start_date
            if valid_dates.mean() > 0.4:
                df = df.rename(columns={col: "start_date"})
                ser = parsed.copy()

                # if tz-aware, make tz-naive first (only if tz is present)
                if getattr(ser.dt, "tz", None) is not None:
                    ser = ser.dt.tz_convert(None)

                # normalize to date-only and format as YYYY-MM-DD
                df["start_date"] = ser.dt.normalize().dt.strftime("%Y-%m-%d")

            # Verify that rows contain ship type
            valid_ship = s.isin(['2DAY', 'GROUND', 'FREIGHT', 'EXPRESS'])
            # if 80% of rows match we rename the column to shipment_type
            if valid_ship.mean() > 0.8:
                df = df.rename(columns={col: "shipment_type"})
                st = df["shipment_type"].astype(str).str.strip()
                df.loc[~st.isin(['2DAY', 'GROUND', 'FREIGHT', 'EXPRESS']), 'shipment_type'] = pd.NA

        columns_required = ['invoice_id', 'client_id', 'start_date', 'subtotal', 'tax', 'total', 'shipment_type']
        for c in columns_required:
            if c not in df.columns:
                df[c] = df[c] = pd.NA
        df = df[columns_required]
        cleaned.append(df)
    return cleaned


def combine_single_clients(dfs):
    if not dfs:
        return pd.DataFrame(columns=['client_id', 'client_name', 'start_date', 'status', 'tier'])
    df = pd.concat(dfs, ignore_index=True)

    def pick_first_nonnull(s):
        s = s.dropna()
        return s.iloc[0] if len(s) else pd.NA

    # grouped by client_id and removed id C10456
    df_ret = (
        df.groupby(['client_id'], as_index=False)
        .agg(
            status=('status', pick_first_nonnull),
            tier=('tier', pick_first_nonnull),
            start_date=('start_date', pick_first_nonnull),
            client_name=('client_name', pick_first_nonnull)
        )
        .sort_values(['client_id', 'start_date'])
        .reset_index(drop=True)
    )
    columns_required = ['client_id', 'client_name', 'status', 'start_date', 'tier']
    df_ret = df_ret[columns_required]
    print(f"Successfully Cleaned and Merged Clients Files... \n")
    return df_ret


def combine_single_invoices(dfs):
    if not dfs:
        return pd.DataFrame(columns=['invoice_id', 'client_id', 'start_date', 'subtotal', 'tax', 'total',
                                     'shipment_type'])
    df = pd.concat(dfs, ignore_index=True)

    def pick_first_nonnull(s):
        s = s.dropna()
        return s.iloc[0] if len(s) else pd.NA

    # grouped by invoice_id and client_id
    df_ret = (
        df.groupby(['invoice_id'], as_index=False, dropna=False)
        .agg(
            start_date=('start_date', pick_first_nonnull),
            client_id=('client_id', pick_first_nonnull),
            subtotal=('subtotal', pick_first_nonnull),
            tax=('tax', pick_first_nonnull),
            total=('total', pick_first_nonnull),
            shipment_type=('shipment_type', pick_first_nonnull)
        )
        .sort_values(['invoice_id', 'client_id'])
        .reset_index(drop=True)
    )
    columns_required = ['invoice_id', 'client_id', 'start_date', 'subtotal', 'tax', 'total', 'shipment_type']
    df_ret = df_ret[columns_required]
    print(f"Successfully Cleaned and Merged Invoices Files... \n")
    return df_ret
