"""
Invoice–Client Merger Utility.

This module defines a function that merges client information with
invoice data to produce a consolidated view of invoices enriched
with client attributes.

Author: Guy Kogan
"""


def combine(clients, invoices):
    """
        Combine client attributes into invoice rows to produce a final invoice–client model.

        The function:
          1) De-duplicates client attributes by `client_id`
          2) Left-joins invoices to those client attributes
          3) Drops unneeded financial columns and client columns that shouldn't be duplicated
          4) Returns a curated set of output columns

        Parameters
        ----------
        clients : pandas.DataFrame
            Client table expected to contain at least:
            ['client_id', 'client_name', 'status', 'tier'].
        invoices : pandas.DataFrame
            Invoice table expected to contain at least:
            ['client_id', 'invoice_id', 'start_date', 'total', 'shipment_type'] and may include
            ['tax', 'subtotal'] which will be dropped.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with columns:
            ['client_id', 'client_name', 'invoice_id', 'start_date', 'total', 'shipment_type'].

        Notes
        -----
        - Uses a left join to keep all invoice rows even if client attributes are missing.
        - Any unexpected/missing columns will raise a KeyError; adjust the required sets below if schemas evolve.
        """
    right = clients[['client_id', 'client_name', 'status', 'tier']].drop_duplicates('client_id')
    merged = invoices.merge(
        right,
        on="client_id",
        how="left",
        suffixes=("", "_client")
    )
    merged = merged.drop(columns=['tax', 'subtotal', 'tier', 'status'])
    columns_required = ['client_id', 'client_name', 'invoice_id', 'start_date', 'total', 'shipment_type']
    print(f"Successfully Created final invoice client model... \n")
    return merged[columns_required]
