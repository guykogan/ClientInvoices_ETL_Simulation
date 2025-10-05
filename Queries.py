import pandas as pd


def invoice_amount_sorted(df_query):
    df = df_query.copy()
    top_five = (df.groupby(['client_id', 'client_name'], as_index=False)['total'].sum().
                sort_values('total', ascending=False))
    return top_five


def month_over_month_growth(df_query):
    df = df_query.copy()
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    mask = df['start_date'].between('2024-01-01', '2025-12-31', inclusive='both')
    df = df.loc[mask].copy()
    df['year_month'] = df['start_date'].dt.to_period('M').astype(str)

    m = (df.groupby(['client_id', 'client_name', 'year_month'], as_index=False)['total']
         .sum()
         .sort_values(['client_id', 'year_month']))

    # absolute change per month
    m['mom_delta'] = m.groupby('client_id')['total'].diff()
    # change in percent
    m['mom_growth_pct'] = m.groupby('client_id')['total'].pct_change() * 100

    # change from NaN in first month to 0 percent change
    m.loc[0, 'mom_delta'] = 0
    m.loc[0, 'mom_growth_pct'] = 0
    return m


def discount_applied(df_query):
    df = df_query.copy()
    disc = df['shipment_type'].map({'GROUND': 0.8, 'FREIGHT': 0.7, '2DAY': 0.5}).fillna(1.0)
    df['total'] = df['total'] * disc
    df = invoice_amount_sorted(df)
    return df


def reclassify_discount(df_query):
    df = df_query.copy()
    old_prices = invoice_amount_sorted(df)
    df['shipment_type'] = df['shipment_type'].replace({'EXPRESS': 'GROUND'})
    new_prices = discount_applied(df)
    # make sure merge occurs on keys instead of row by row
    new_prices = new_prices.merge(old_prices, on=['client_id', 'client_name'], how='inner').fillna(0)
    new_prices = new_prices.rename(columns={'total_x': 'discounted_total', 'total_y': 'old_total'})
    new_prices['savings'] = new_prices['old_total'] - new_prices['discounted_total']
    new_prices['percent_savings'] = new_prices['savings'] / new_prices['old_total'] * 100
    columns_required = ['client_id', 'client_name', 'old_total', 'discounted_total', 'savings', 'percent_savings']
    # bend to order of my liking
    new_prices = new_prices[columns_required]
    return new_prices, new_prices[new_prices['percent_savings'] > 50.0], new_prices[new_prices['savings'] > 500_000.0]
