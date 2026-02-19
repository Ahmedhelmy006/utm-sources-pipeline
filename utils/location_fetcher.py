import aiohttp
from bs4 import BeautifulSoup
from rich.console import Console
import asyncio
import os
import sys


from config.headers import headers
from utils.location_identifier import LocationIdentifier

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class LocationFetcher:
    def __init__(self):
        """Initialize the async location fetcher with shared session and console"""
        self.console = Console()
        self.headers = headers
    
    async def fetch_location(self, session, subscriber_id):
        """
        Fetch location data for a single subscriber asynchronously
        
        Args:
            session (aiohttp.ClientSession): Shared HTTP session
            subscriber_id (str): The subscriber ID to fetch location for
            
        Returns:
            tuple: (subscriber_id, city, state, country)
        """
        url = f"https://app.kit.com/subscribers/{subscriber_id}"
        try:
            self.console.print(f"[yellow]Fetching location for subscriber {subscriber_id}...")
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    html = await response.text()
                    city, state = self.clean_response(html)
                    country = "N/A"
                    if city and state:
                        try:
                            identifier = LocationIdentifier(city=city, state=state)
                            country = identifier.search_with_handler()
                            self.console.print(f"[green]Found country for {subscriber_id}: {country}")
                        except Exception as e:
                            self.console.print(f"[red]Error identifying country for {subscriber_id}: {e}")
                    return subscriber_id, city, state, country
                else:
                    self.console.print(f"[red]Failed to fetch location for {subscriber_id}: {response.status}")
                    return subscriber_id, None, None, "N/A"
        except Exception as e:
            self.console.print(f"[red]Error fetching location for {subscriber_id}: {e}")
            return subscriber_id, None, None, "N/A"

    def clean_response(self, html):
        """Extract city and state from HTML response"""
        soup = BeautifulSoup(html, 'html.parser')
        locations = soup.find(attrs={"data-city": True, "data-state": True})
        if locations:
            city = locations['data-city']
            state = locations['data-state']
            return city, state
        return None, None
    
    async def fetch_all_locations(self, subscriber_ids, max_concurrent=3):
        """
        Fetch location data for multiple subscribers concurrently
        
        Args:
            subscriber_ids (list): List of subscriber IDs
            max_concurrent (int): Maximum number of concurrent requests
            
        Returns:
            dict: Mapping of subscriber_id to location data
        """
        results = {}
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(subscriber_id):
            async with semaphore:
                return await self.fetch_location(session, subscriber_id)
        
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_with_semaphore(sid) for sid in subscriber_ids]
            for completed_task in asyncio.as_completed(tasks):
                sid, city, state, country = await completed_task
                results[sid] = {"city": city, "state": state, "country": country}
        
        return results
    
