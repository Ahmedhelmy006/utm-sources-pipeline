import aiohttp
import asyncio
from rich.console import Console
from utils.helpers import *

class ReferrerInfoFetcher:
    def __init__(self, headers, base_url="https://app.kit.com/subscribers"):
        self.headers = headers
        self.base_url = base_url
        self.console = Console()

    async def fetch_referrer_info(self, session, subscriber_id):
        url = f"{self.base_url}/{subscriber_id}/referrer_info"
        try:
            async with session.get(url, headers=self.headers) as response:
                if response.status != 200:
                    self.console.print(f"[red]Failed for ID {subscriber_id}: {response.status}")
                    return subscriber_id, None

                referrer_info = await response.json()
                subscriber_fields = get_subscribers_fields(subscriber_id)
                if referrer_info["referrer_utm"]["source"] == "" :
                       
                   referrer_info["referrer_utm"]["source"] , referrer_info["referrer_utm"]["medium"] ,referrer_info["referrer_utm"]["content"], referrer_info["referrer_utm"]["campaign"] = extract_utms(subscriber_fields)
                
                return referrer_info
            
        except Exception as e:      
            self.console.print(f"[red]Error fetching {subscriber_id}: {e}")
            return subscriber_id, None

        except Exception as e:
            self.console.print(f"[red]Error fetching {subscriber_id}: {e}")
            return subscriber_id, None

    async def fetch_all_referrer_info(self, subscriber_ids, max_concurrent=3):
        results = {}
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_with_semaphore(subscriber_id):
            async with semaphore:
                return await self.fetch_referrer_info(session, subscriber_id)
        
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_with_semaphore(sid) for sid in subscriber_ids]
        
            for completed_task in asyncio.as_completed(tasks):
                sid, referrer_info = await completed_task
                results[sid] = referrer_info
        
        return results

if __name__ == "__main__": 
    from config.headers import headers
    referrer = ReferrerInfoFetcher(headers=headers)
    async def main():
        async with aiohttp.ClientSession() as session:
            result = await referrer.fetch_referrer_info(session, 3933021561)
            print(result)
    asyncio.run(main())