import asyncio, aiohttp
import os
from dotenv import load_dotenv
from config.settings import base_url

load_dotenv()

async def get_subscribers_fields(subscriber_id):
    """
    ARGS: subscriber_id
    Returns All tags for a subscriber. Pretty simple, huh?
    """
    url = base_url + f"/subscribers/{subscriber_id}"
    headers = {
            'Accept': 'application/json',
            'X-Kit-Api-Key': os.getenv("KIT_V4_API_KEY")
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url = url, headers = headers) as response:
            return await response.json()
    
async def extract_utms(subscriber_data):
    """
    ARGS: A JSON of subscribers data. Usually: {subscriber: {id: ''...., fields: {}}}
    The goal is to extract the utm values from those fields. 
    
    Returns: UTM_source, UTM_Medium, UTM_Campaign, UTM_Content  
    """
    source = subscriber_data["subscriber"]["fields"]["utm_source"]
    medium = subscriber_data["subscriber"]["fields"]["utm_medium"]
    campaign = subscriber_data["subscriber"]["fields"]["utm_campaign"]
    content = subscriber_data["subscriber"]["fields"]["utm_content"]
    return  source, medium, campaign, content
    

if __name__ == "__main__":
    data = asyncio.run(get_subscribers_fields(3924805912))
    print(asyncio.run(extract_utms(data)))