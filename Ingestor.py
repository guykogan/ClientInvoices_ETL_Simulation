import pandas as pd
import os

clients_information_list = []
clients_invoices = []


def ingest(directory_name):
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
