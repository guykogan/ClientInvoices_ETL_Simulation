def combine(clients, invoices):
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
