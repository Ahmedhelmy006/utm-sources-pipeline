import aiohttp
import asyncio
from rich.console import Console
from dotenv import load_dotenv
import os

load_dotenv()

class SubscriberFetcher:
    def __init__(self, base_url="https://api.kit.com/v4"):
        """
        Initialize AsyncSubscriberFetcher with API key and base URL.

        Args:
            env_manager (EnvironmentManager): Instance to fetch environment variables.
            base_url (str): API base URL (default is https://api.kit.com/v4).
        """
        self.api_key = os.getenv("KIT_V4_API_KEY")
        self.base_url = base_url
        self.headers = {
            'Accept': 'application/json',
            'X-Kit-Api-Key': self.api_key
        }
        self.console = Console()

    async def fetch_subscribers(self, starting_date, ending_date, per_page=500, max_records=15000):
        """
        Fetch all subscribers from the API within the date range asynchronously.

        Args:
            starting_date (str): Start date in ISO format (e.g., "2025-01-05T00:00:00Z").
            ending_date (str): End date in ISO format (e.g., "2025-01-05T23:59:59Z").
            per_page (int): Number of records per page.
            max_records (int): Maximum records to fetch.

        Returns:
            list: List of subscriber dictionaries.
        """
        params = {
            "created_after": starting_date,
            "created_before": ending_date,
            "per_page": per_page,
            "status": 'active'
        }
        subscribers = []
        next_page_cursor = None

        self.console.print(f"[bold yellow]Fetching subscribers from {starting_date} to {ending_date}")
        
        async with aiohttp.ClientSession() as session:
            while len(subscribers) < max_records:
                if next_page_cursor:
                    params['after'] = next_page_cursor
                
                try:
                    async with session.get(f"{self.base_url}/subscribers", headers=self.headers, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            new_subscribers = data.get('subscribers', [])
                            subscribers.extend(new_subscribers)
                            
                            pagination = data.get('pagination', {})
                            if pagination.get('has_next_page') and len(subscribers) < max_records:
                                next_page_cursor = pagination.get('end_cursor')
                                self.console.print(f"[yellow]Fetched {len(subscribers)} subscribers so far, getting next page...")
                            else:
                                break
                        else:
                            error_text = await response.text()
                            self.console.print(f"[red]Failed to fetch data. Status code: {response.status}")
                            self.console.print(f"[red]Error response: {error_text}")
                            break
                except Exception as e:
                    self.console.print(f"[red]Error during subscriber fetch: {e}")
                    break

        self.console.print(f"[bold green]Successfully fetched {len(subscribers)} subscribers")
        return subscribers[:max_records]

    async def filter_subscribers(self, subscribers):
        """
        Filter and transform subscriber data.

        Args:
            subscribers (list): List of raw subscriber dictionaries.

        Returns:
            list: Filtered list of subscribers with selected fields.
        """
        filtered_subscribers = []
        for subscriber in subscribers:
            filtered_subscribers.append({
                "id": subscriber.get('id'),
                "created_at": subscriber.get('created_at'),
                "name": subscriber.get('first_name'),
                "email": subscriber.get('email_address'),
                "status": subscriber.get('state'),
                "location_state": None,  # Will be populated later
                "location_country": None  # Will be populated later
            })
        
        self.console.print(f"[green]Filtered {len(filtered_subscribers)} subscribers")
        return filtered_subscribers