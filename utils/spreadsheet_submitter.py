import pandas as pd
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

class SpreadsheetSubmitter:
    def __init__(self, credentials_path, spreadsheet_id, tab_name):
        """
        Initialize the SpreadsheetSubmitter.

        Args:
            credentials_path (str): Path to the Google Service Account credentials JSON file.
            spreadsheet_id (str): ID of the Google Spreadsheet.
            tab_name (str): Name of the tab to append data.
        """
        self.credentials = Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        self.spreadsheet_id = spreadsheet_id
        self.tab_name = tab_name

    def write_to_google_sheet(self, data, column_order):
        # Convert data to DataFrame and align with column order
        data_frame = pd.DataFrame(data)
        data_frame = data_frame[column_order]
        data_frame = data_frame.fillna("N/A")  # Replace None values with 'N/A'


        service = build('sheets', 'v4', credentials=self.credentials)
        sheet = service.spreadsheets()

        # Fetch existing rows to calculate the starting row
        range_name = f"{self.tab_name}!A1:Q"
        result = sheet.values().get(spreadsheetId=self.spreadsheet_id, range=range_name).execute()
        values = result.get('values', [])
        start_row = len(values) + 1

        # Prepare data for appending
        data_to_write = [data_frame.columns.tolist()] if start_row == 1 else []  # Add header if sheet is empty
        data_to_write += data_frame.values.tolist()


        range_to_write = f"{self.tab_name}!A{start_row}"
        print(f"Writing to range: {range_to_write}")

        # Append data to the Google Sheet
        request = sheet.values().append(
            spreadsheetId=self.spreadsheet_id,
            range=range_to_write,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": data_to_write}
        )
        response = request.execute()
        print(f"Data appended successfully to tab '{self.tab_name}'.")
