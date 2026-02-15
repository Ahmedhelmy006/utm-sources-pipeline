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
                
                # 1. Standard Extraction
                root_utm = referrer_info.get('referrer_utm') or {}
                origin_utm = referrer_info.get('origin', {}).get('referrer_utm') or {}

                if not root_utm.get("source") and origin_utm.get("source"):
                    referrer_info['referrer_utm'] = origin_utm.copy()
                
                utms_root = referrer_info.setdefault('referrer_utm', {})

                # 2. API Fallback (Fetching custom fields)
                if not utms_root.get("source"):
                    self.console.print(f"[yellow]Source missing for {subscriber_id}, extracting through the API")
                    sub_data = await get_subscribers_fields(subscriber_id)
                    s, m, c, con = await extract_utms(sub_data)
                    
                    utms_root.update({"source": s, "medium": m, "campaign": c, "content": con})

                # 3. THE "WTF" FIX: Last Resort Fallback
                # If still no source, use the 'origin' name (e.g., "100 Copilot Tips")
                if not utms_root.get("source"):
                    origin_name = referrer_info.get('origin', {}).get('name')
                    if origin_name:
                        self.console.print(f"[cyan]Using Origin Name as fallback: {origin_name}")
                        utms_root["source"] = origin_name
                        utms_root["medium"] = "organic_form" # Label it so you know it wasn't a true UTM

                # 4. Final Logging
                if not utms_root.get("source"):
                    self.console.print(f"[dim white]Absolutely no source found for {subscriber_id}")
                else:
                    self.console.print(f"[green]{subscriber_id} Found ({utms_root['source']}). Proceeding.")
                        
                return subscriber_id, referrer_info

        except Exception as e:
            self.console.print(f"[red]Error fetching {subscriber_id}: {e}")
            return subscriber_id, None

        except Exception as e:
            self.console.print(f"[red]Error fetching {subscriber_id}: {e}")
            return subscriber_id, None

    async def fetch_all_referrer_info(self, subscriber_ids, max_concurrent=3):
            results = {}
            connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=max_concurrent)
            timeout = aiohttp.ClientTimeout(total=15)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                semaphore = asyncio.Semaphore(max_concurrent)
                
                async def fetch_with_semaphore(subscriber_id):
                    async with semaphore:
                        return await self.fetch_referrer_info(session, subscriber_id)
                
                tasks = [fetch_with_semaphore(sid) for sid in subscriber_ids]
                for completed_task in asyncio.as_completed(tasks):
                    try:
                        sid, referrer_info = await completed_task
                        results[sid] = referrer_info
                    except Exception as e:
                        self.console.print(f"[red]Task failed: {e}")
                        
            return results

if __name__ == "__main__": 
    from config.headers import headers
    referrer = ReferrerInfoFetcher(headers=headers)
    async def main():
        async with aiohttp.ClientSession() as session:
            result = await referrer.fetch_referrer_info(session, 3931717822)
            print(result)
    asyncio.run(main())