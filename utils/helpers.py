import asyncio, aiohttp
import os
from dotenv import load_dotenv
from config.settings import base_url
import requests
import re
from config.settings import facebook_ads_campaigns

load_dotenv()

def get_subscribers_fields(subscriber_id):
    """
    ARGS: subscriber_id
    Returns All tags for a subscriber. Pretty simple, huh?
    """
    url = base_url + f"/subscribers/{subscriber_id}"
    headers = {
            'Accept': 'application/json',
            'X-Kit-Api-Key': os.getenv("KIT_V4_API_KEY")
    }

    response = requests.get(url = url, headers = headers)
    return  response.json()
    
def extract_utms(subscriber_data):
    """
    ARGS: A JSON of subscribers data. Usually: {subscriber: {id: ''...., fields: {}}}
    The goal is to extract the utm values from those fields. 
    
    Returns: UTM_source, UTM_Medium, UTM_Campaign, UTM_Content  
    """
    pattern = r'\b\d{11,}\b'
    
    source = subscriber_data["subscriber"]["fields"].get("utm_source") or ""
    medium = subscriber_data["subscriber"]["fields"].get("utm_medium") or ""
    campaign = subscriber_data["subscriber"]["fields"].get("utm_campaign") or ""
    content = subscriber_data["subscriber"]["fields"].get("utm_content") or ""

    if re.search(pattern, source) or source == "fb":
        source = "facebook-ads"
    
    if re.search(pattern, campaign):
        campaign = "facebook-ads"
    
    elif medium in facebook_ads_campaigns:
        medium = 'paid-ads'
    
    return source, medium, campaign, content
    
def unify_sources():
    pass

if __name__ == "__main__":
    data = get_subscribers_fields(3949825539)
    print(extract_utms(data))