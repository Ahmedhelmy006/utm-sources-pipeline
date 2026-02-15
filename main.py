import asyncio
import sys,os
from datetime import datetime, timedelta, timezone
from rich.console import Console
from dotenv import load_dotenv
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from concurrent.futures import ThreadPoolExecutor
import time


from utils.spreadsheet_submitter import SpreadsheetSubmitter
from utils.data_mapper import DataMapper
from config.headers import headers

# Import the new async classes
from utils.subscriber_fetcher import SubscriberFetcher
from utils.location_fetcher import LocationFetcher
from utils.referrer_fetcher import ReferrerInfoFetcher

load_dotenv()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class AsyncMainRunner:
    def __init__(self):
        self.console = Console()
        self.subscriber_fetcher = SubscriberFetcher()
        self.headers = headers
        
        self.referrer_info_fetcher = ReferrerInfoFetcher(headers=self.headers)   
        self.location_fetcher = LocationFetcher()
        
        self.spreadsheet_submitter = SpreadsheetSubmitter(credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH"), 
                                                          spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID"), 
                                                          tab_name = os.getenv("GOOGLE_TAB_NAME"))

    def parse_date(self, date_str):
        """Parse date string in MM/DD/YYYY format to datetime object"""
        try:
            return datetime.strptime(date_str, "%m/%d/%Y").replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                # Try alternative format DD/MM/YYYY
                return datetime.strptime(date_str, "%d/%m/%Y").replace(tzinfo=timezone.utc)
            except ValueError:
                # Try YYYY-MM-DD format
                return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)


    async def process_single_day(self, current_date):
        """Process data for a single day"""
        day_start = current_date.strftime("%Y-%m-%dT00:00:00Z")
        day_end = current_date.replace(hour=23, minute=59, second=59).strftime("%Y-%m-%dT23:59:59Z")
        
        self.console.print(f"[bold blue]Processing {current_date.strftime('%Y-%m-%d')}...")
        
        subscribers = await self.subscriber_fetcher.fetch_subscribers(day_start, day_end)
        filtered_subscribers = await self.subscriber_fetcher.filter_subscribers(subscribers)
        
        if not filtered_subscribers:
            self.console.print(f"[yellow]No subscribers found for {current_date.strftime('%Y-%m-%d')}")
            return 0
        
        subscriber_ids = [sub["id"] for sub in filtered_subscribers]
        self.console.print(f"[bold yellow]Processing {len(subscriber_ids)} subscribers for {current_date.strftime('%Y-%m-%d')}...")
        
        locations_task = asyncio.create_task(
            self.location_fetcher.fetch_all_locations(subscriber_ids, max_concurrent=3)
        )
        referrers_task = asyncio.create_task(
            self.referrer_info_fetcher.fetch_all_referrer_info(subscriber_ids, max_concurrent=3)
        )
        
        locations = await locations_task
        referrers = await referrers_task
        
        for subscriber in filtered_subscribers:
            sub_id = subscriber["id"]
            if sub_id in locations:
                location_data = locations[sub_id]
                subscriber["location_state"] = location_data.get("state", "")
                subscriber["location_country"] = location_data.get("country", "")
            
            if sub_id in referrers:
                subscriber["referrer_info"] = referrers[sub_id]
        
        combined_data = DataMapper.combine_data(filtered_subscribers)
        
        # Submit to Google Sheets
        column_order = [
            "subscriber_created_at",
            "subscriber_state",
            "subscriber_email",
            "referrer_name",
            "referrer_domain",
            "referrer_utm_source",
            "referrer_utm_medium",
            "referrer_utm_campaign",
            "referrer_utm_content",
            "subscriber_physical_state",
            "subscriber_country",
            "Subscriber Region",
            "Subscriber Purchase Power",
            "Subscriber Purchase Score"
        ]
        
        self.console.print(f"[yellow]Submitting {len(filtered_subscribers)} records to Google Sheets for {current_date.strftime('%Y-%m-%d')}...")
        self.spreadsheet_submitter.write_to_google_sheet(combined_data, column_order)
        
        self.console.print(f"[bold green]Successfully processed {len(filtered_subscribers)} subscribers for {current_date.strftime('%Y-%m-%d')}")
        
        return len(filtered_subscribers)

    async def run(self, start_date_str=None, end_date_str=None):
        """Main method to orchestrate the data collection and processing"""
        start_time = time.time()
        
        # Handle date range
        if start_date_str and end_date_str:
            try:
                start_date = self.parse_date(start_date_str).replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = self.parse_date(end_date_str).replace(hour=0, minute=0, second=0, microsecond=0)
                
                # Calculate number of days
                total_days = (end_date - start_date).days + 1
                self.console.print(f"[bold cyan]Processing data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({total_days} day(s))...")
            except ValueError as e:
                self.console.print(f"[bold red]Error parsing dates: {e}")
                self.console.print("[yellow]Please use format MM/DD/YYYY, DD/MM/YYYY, or YYYY-MM-DD")
                return
        else:
            # Default behavior: Yesterday's date
            start_date = (datetime.now(timezone.utc) - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date
            
            self.console.print(f"[bold cyan]Processing data for {start_date.strftime('%Y-%m-%d')} (default: yesterday)...")
        
        
        # Process each day individually
        current_date = start_date
        total_processed = 0
        
        while current_date <= end_date:
            try:
                daily_count = await self.process_single_day(current_date)
                total_processed += daily_count
                
                if current_date < end_date:
                    await asyncio.sleep(1)
                    
            except Exception as e:
                self.console.print(f"[bold red]Error processing {current_date.strftime('%Y-%m-%d')}: {e}")
            
            current_date += timedelta(days=1)
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        self.console.print(f"[bold green]Process completed in {elapsed_time:.2f} seconds")
        self.console.print(f"[bold green]Successfully processed {total_processed} total subscribers across all days")


async def main(start_date_str=None, end_date_str=None):
    runner = AsyncMainRunner()
    await runner.run(start_date_str, end_date_str)

if __name__ == "__main__":
    asyncio.run(main())
