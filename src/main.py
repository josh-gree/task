import requests
import aiohttp
import asyncio
import pprint

from typing import List
from math import ceil

from pandas import DataFrame

from functools import reduce
from operator import add
from collections import Counter


API_ROOT = 'https://swapi.dev/api/'
API_PLANETS = API_ROOT + 'planets'
API_PEOPLE = API_ROOT + 'people'

RESULTS_PATH = '/app/results'

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


def planet_processing(response: dict):

    results = response['results']

    return [
        {
            'name': res['name'],
            'count': len(res['residents'])
        }

        for res in results
    ] 

def people_processing(response: dict):

    results = response['results']

    home_worlds = [res['homeworld'] for res in results]

    c = Counter(home_worlds)

    return c

def planet_name_processing(response: dict):

    return response['name']


async def fetch(url: str, session: aiohttp.ClientSession, processing_func) -> List[dict]:
    """Fetch json data from URL extract results key and for each 
    record return name of planet and count of residents

    Args:
        url (str): url from which to extract result
        session (aiohttp.ClientSession)

    Returns:
        List[dict]: [{'name','count'},...]
    """
    async with session.get(url) as response:
        response = await response.json()

        return processing_func(response)

async def fetch_all(urls: List[str], processing_func) -> List[dict]:
    """Run multiple fetches asynchronously for each url in urls

    Args:
        urls (List[str]): urls to fetch 

    Returns:
        List[dict]: resulting data
    """
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            task = asyncio.create_task(fetch(url, session, processing_func))
            tasks.append(task)

        resps = asyncio.gather(*tasks)
        await resps

    return resps.result()


if __name__ == '__main__':


    planet_pages = generate_all_page_urls(API_PLANETS)
    people_pages = generate_all_page_urls(API_PEOPLE)

    resps_planets = asyncio.run(fetch_all(planet_pages, planet_processing))
    result = reduce(add,resps_planets)

    df = DataFrame.from_dict(result)
    df.to_csv(RESULTS_PATH + '/planets.csv',index=False)

    resps_people = asyncio.run(fetch_all(people_pages, people_processing))
    result = reduce(add,resps_people)

    resps_planet_names = asyncio.run(fetch_all(list(result.keys()), planet_name_processing))
    result = dict(zip(resps_planet_names,list(result.values())))

    df = DataFrame.from_dict(result, orient='index')
    df.to_csv(RESULTS_PATH + '/people.csv')