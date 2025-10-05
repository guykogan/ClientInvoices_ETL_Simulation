"""
Invoice analytics utilities.

This module provides helper functions to:
- Aggregate totals per client and sort by invoice amount
- Compute month-over-month growth for client totals
- Apply shipping-based discounts and re-aggregate
- Reclassify discounts and compute savings vs. original totals

Author: Guy Kogan
"""
import pandas as pd


def invoice_amount_sorted(df_query):
    """
        Aggregate invoice totals per client and return a sorted DataFrame.

        Parameters
        ----------
        df_query : pandas.DataFrame
            Input invoice–client model with at least:
            ['client_id', 'client_name', 'total'].

        Returns
        -------
        pandas.DataFrame
            Per-client totals, sorted descending by 'total'.
            Columns: ['client_id', 'client_name', 'total'].
        """
    df = df_query.copy()
    top_five = (df.groupby(['client_id', 'client_name'], as_index=False)['total'].sum().
                sort_values('total', ascending=False))
    return top_five


def month_over_month_growth(df_query):
    """
        Compute month-over-month absolute and percentage growth per client.

        The function:
          - Coerces 'start_date' to datetime
          - Filters to the window 2024-01-01 - 2025-12-31 (inclusive)
          - Aggregates totals per client per month
          - Adds 'mom_delta' and 'mom_growth_pct' columns

        Parameters
        ----------
        df_query : pandas.DataFrame
            Input invoice–client model with at least:
            ['client_id', 'client_name', 'start_date', 'total'].

        Returns
        -------
        pandas.DataFrame
            Columns include:
            ['client_id', 'client_name', 'year_month', 'total', 'mom_delta', 'mom_growth_pct'].
        """
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
    """
        Apply shipment-type discounts to 'total' and re-aggregate per client.

        Discounts:
          - GROUND: 20% off (x 0.8)
          - FREIGHT: 30% off (x 0.7)
          - 2DAY: 50% off (x 0.5)
          - Other/unknown shipment types: no discount (x 1.0)

        Parameters
        ----------
        df_query : pandas.DataFrame
            Input invoice–client model with at least:
            ['client_id', 'client_name', 'shipment_type', 'total'].

        Returns
        -------
        pandas.DataFrame
            Per-client totals after discount, sorted descending.
        """
    df = df_query.copy()
    disc = df['shipment_type'].map({'GROUND': 0.8, 'FREIGHT': 0.7, '2DAY': 0.5}).fillna(1.0)
    df['total'] = df['total'] * disc
    df = invoice_amount_sorted(df)
    return df


def reclassify_discount(df_query):
    """
        Reclassify shipment types and compute savings vs. the original totals.

        This function:
          1) Computes per-client totals from the original data (old prices).
          2) Reclassifies 'EXPRESS' to 'GROUND' (example business rule).
          3) Applies discounts and re-aggregates per client (new prices).
          4) Calculates absolute and percent savings vs. original totals.
          5) Returns:
             - The full savings table
             - Rows with percent savings > 50%
             - Rows with absolute savings > 500,000

        Parameters
        ----------
        df_query : pandas.DataFrame
            Invoice–client model with at least:
            ['client_id', 'client_name', 'shipment_type', 'total'].

        Returns
        -------
        (pandas.DataFrame, pandas.DataFrame, pandas.DataFrame)
            - full_savings: columns
              ['client_id', 'client_name', 'old_total', 'discounted_total', 'savings', 'percent_savings']
            - pct_gt_50: rows from full_savings with percent_savings > 50.0
            - savings_gt_500k: rows from full_savings with savings > 500_000.0
        """
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
