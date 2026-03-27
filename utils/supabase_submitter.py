from supabase import create_client
from dotenv import load_dotenv
import os

load_dotenv()

class SupabaseSubmitter:
    def __init__(self, df):
        self.df = df

    def establish_connection(self):
        supabase_api_url = os.getenv("SUPABASE_PROJECT_URL")
        supabase_api_key = os.getenv("SUPABASE_PROJECT_KEY")
        return create_client(supabase_api_url, supabase_api_key)
    
    def submit_df(self):
        client = self.establish_connection()
        try:
            records = self.df.to_dict(orient="records")
            response = client.table("kit_subscribers").insert(records).execute()
            print("Successfully appended data to Supabase: kit_subscribers")
        except Exception as e:
            print(f"Couldn't submit data to Supabase. Error message: {e}")