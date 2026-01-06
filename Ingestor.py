"""
Client & Invoice CSV ingestor.

This module provides a simple ingestion utility that scans a directory for CSV files
matching the naming conventions `clients_*.csv` and `invoices_*.csv`, reads them into
pandas DataFrames, and returns two lists of DataFrames.

Author: Guy Kogan
"""
import pandas as pd
import os

clients_information_list = []
clients_invoices = []



def ingest(directory_name):
    """
        Ingest CSV files from a directory into DataFrame lists.

        Scans the provided directory for files that:
          - start with 'clients_' and end with '.csv' → appended to `clients_information_list`
          - start with 'invoices_' and end with '.csv' → appended to `clients_invoices`

        Files that cannot be read will be skipped with a warning.

        Parameters
        ----------
        directory_name : str
            Path to the directory containing the CSV files.

        Returns
        -------
        Tuple[List[pd.DataFrame], List[pd.DataFrame]]
            A tuple of (clients_information_list, clients_invoices), where each is a list
            of pandas DataFrames corresponding to the successfully read CSVs.

        Notes
        -----
        - Uses `on_bad_lines='warn'` to continue through malformed rows while logging.
        - This function mutates the global lists defined in this module.
        """
    # get current file list in current director
    files_in_current_dir = os.listdir(directory_name)

    # loop through files looking for csv and organize by naming convention (clients_ or invoices_)
    for file in files_in_current_dir:
        try:
            if '.csv' in file and 'clients_' in file:
                clients_information_list.append(pd.read_csv(file, on_bad_lines="warn"))
            elif '.csv' in file and 'invoices_' in file:
                clients_invoices.append(pd.read_csv(file, on_bad_lines="warn"))
        except Exception as e:
            print(f'File {file} was unable to be read due to error: {repr(e)}')

    # Checks if at least 1 file was successfully pulled and reports to user on counts
    if len(clients_invoices) > 0 or len(clients_information_list) > 0:
        print(f"Successfully Ingested Files \n"
              f"customer information: {len(clients_information_list)} files\n"
              f"invoices: {len(clients_invoices)} files\n")
    return clients_information_list, clients_invoices
