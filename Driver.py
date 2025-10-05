"""
ETL Driver Script.

Runs the end-to-end pipeline:
  1) Ingest client and invoice CSVs from a directory
  2) Transform/standardize raw frames
  3) Combine into curated client/invoice tables and a joined model
  4) Execute analysis queries and write outputs to disk

Author: Guy Kogan
"""
import os
from Ingestor import ingest
from Transformer import transform_clients, combine_single_clients, transform_invoices, combine_single_invoices
from Model import combine
from Queries import invoice_amount_sorted, month_over_month_growth, discount_applied, reclassify_discount


if __name__ == '__main__':
    """
        Execute the ETL and analysis pipeline.

        Parameters
        ----------
        input_dir : str, optional
            Directory containing the input CSV files. Defaults to current directory.

        Returns
        -------
        int
            Process exit code (0 on success).
        """
    directory_with_files = './'

    # ingests clients, invoices
    clients_information_list, clients_invoices = ingest(directory_with_files)
    # cleans clients
    clean_client_data = transform_clients(clients_information_list)
    # cleans invoices
    clean_invoices_data = transform_invoices(clients_invoices)

    # combines and groups all clients_v into cleaned / merged dataframe
    final_clients_df = combine_single_clients(clean_client_data)
    # combines and groups all invoices_v into cleaned / merged dataframe
    final_invoices_df = combine_single_invoices(clean_invoices_data)
    # left join invoices on client_id = client_id
    final_clients_invoices = combine(final_clients_df, final_invoices_df)

    # Save transformed and combined outputs
    os.makedirs("Outputs", exist_ok=True)
    final_clients_df.to_csv('Outputs/Clients_Merged_Cleaned.csv', index=False)
    final_invoices_df.to_csv('Outputs/Invoices_Merged_Cleaned.csv', index=False)
    final_clients_invoices.to_csv('Outputs/Clients_Invoices_Model.csv', index=False)
    print("Saved cleaned files and models...\n")

    # Analysis queries
    t5 = invoice_amount_sorted(final_clients_invoices).head(5)
    mom = month_over_month_growth(final_clients_invoices)
    dt5 = discount_applied(final_clients_invoices).head(5)
    savings_total, savings_percent_50, savings_500 = reclassify_discount(final_clients_invoices)

    # Save Analysis queries
    os.makedirs("Analysis", exist_ok=True)
    t5.to_csv('Analysis/Top5_Invoice_Outstanding.csv', index=False)
    mom.to_csv('Analysis/Month_Per_Month_Invoice_Growth.csv', index=False)
    dt5.to_csv('Analysis/Top5_Invoice_Discounts.csv', index=False)
    savings_total.to_csv('Analysis/Total_Cost_Savings.csv', index=False)
    savings_percent_50.to_csv('Analysis/Savings_Over_50percent.csv', index=False)
    savings_500.to_csv('Analysis/Savings_Over_500k.csv', index=False)
    print("Saved Analysis queries...\n")


