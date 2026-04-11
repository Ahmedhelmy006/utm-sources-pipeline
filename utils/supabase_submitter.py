from supabase import create_client
from dotenv import load_dotenv
import os
import time
import requests

load_dotenv()

WEBHOOK_URL = os.getenv("WEBHOOK_URL")

class SupabaseSubmitter:
    def __init__(self, df):
        self.df = df

    def establish_connection(self):
        supabase_api_url = os.getenv("SUPABASE_PROJECT_URL")
        supabase_api_key = os.getenv("SUPABASE_PROJECT_KEY")
        return create_client(supabase_api_url, supabase_api_key)

    def trigger_webhook(self):
        print("Sleeping 120 seconds before triggering webhook...")
        time.sleep(120)
        try:
            response = requests.post(
                WEBHOOK_URL,
                json={
                    "event": "supabase_insert_complete",
                    "table": "kit_subscribers",
                    "records_inserted": len(self.df),
                    "status": "success"
                }
            )
            response.raise_for_status()
            print(f"Webhook triggered successfully. Status: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to trigger webhook. Error: {e}")

    def submit_df(self):
        client = self.establish_connection()
        try:
            records = self.df.to_dict(orient="records")
            response = client.table("kit_subscribers").insert(records).execute()
            print("Successfully appended data to Supabase: kit_subscribers")
            self.trigger_webhook()
        except Exception as e:
            print(f"Couldn't submit data to Supabase. Error message: {e}")