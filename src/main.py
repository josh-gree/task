import requests
import aiohttp
import asyncio
import pprint

from typing import List
from math import ceil

from functools import reduce
from operator import add


API_ROOT = 'https://swapi.dev/api/'
API_PLANETS = API_ROOT + 'planets'

PAGINATION_QUERY = '?page={page}'

def get_page_count(endpoint: str) -> int:
    """Determine number of pages from 
    total count of records and count per page 

    Returns:
        int: Total number of pages
    """
    r = requests.get(endpoint)
    data = r.json()
    
    page_count = data['count'] / len(data['results'])
    
    return ceil(page_count)

def generate_all_page_urls(endpoint: str) -> List[str]:
    """Generate list of URLs for each page of the endpoint

    Returns:
        List[str]: page URLs
    """
    page_count = get_page_count(endpoint)
    pages = [endpoint + PAGINATION_QUERY.format(page=i+1) for i in range(page_count)]
    
    return pages


async def fetch(url: str, session: aiohttp.ClientSession) -> List[dict]:
    """Fetch json data from URL extract results key and for each 
    record return name of planet and count of residents

    Args:
        url (str): url from which to extract result
        session (aiohttp.ClientSession)

    Returns:
        List[dict]: [{'name','count'},...]
    """
    async with session.get(url) as response:
        data = await response.json()

        results = data['results']

        return [
            {
                'name': res['name'],
                'count': len(res['residents'])
            }

            for res in results
        ] 

async def fetch_all(urls: List[str]) -> List[dict]:
    """Run multiple fetches asynchronously for each url in urls

    Args:
        urls (List[str]): urls to fetch 

    Returns:
        List[dict]: resulting data
    """
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            task = asyncio.create_task(fetch(url, session))
            tasks.append(task)

        resps = asyncio.gather(*tasks)
        await resps

    return resps.result()


if __name__ == '__main__':


    planet_pages = generate_all_page_urls(API_PLANETS)

    resps = asyncio.run(fetch_all(planet_pages))
    pprint.pprint(reduce(add,resps))