import asyncio
from typing import Optional, Dict

from aiohttp import ClientSession
from bs4 import BeautifulSoup

TENCHAT_BASE_URL = 'https://tenchat.ru/gostinder/api/web/post/user/username'
TENCHAT_BASE_SIZE = 9


async def fetch_user_data(domain: str, **kwargs) -> Optional[Dict]:
    base_url = f'https://api.{domain}/v2.7/subsite'
    params = {'markdown': 'False', **kwargs}

    async with ClientSession() as session:
        async with session.get(base_url, params=params) as response:
            if not response.ok:
                return None

            result = await response.json()
            return result['result']


async def fetch_tenchat_user_name(user_url: str) -> Optional[str]:
    async with ClientSession() as session:
        async with session.get(user_url) as response:
            if not response.ok:
                return None

            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

            name_element = soup.find('h1', {'data-cy': 'name'})
            return name_element.get_text(separator=' ', strip=True)


async def is_valid_tenchat_user(user_url: str) -> bool:
    async with ClientSession() as session:
        async with session.get(user_url) as response:
            if not response.ok:
                return True

            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

            blocked_div = soup.find('div', {'data-cy': 'blocked'})
            return blocked_div is None


async def fetch_user_posts(domain: str, user_id: int, posts_amount: Optional[int] = None):
    base_url = f'https://api.{domain}/v2.8/timeline'
    params = {'markdown': 'false', 'sorting': 'new', 'subsitesIds': user_id}

    posts = []
    async with ClientSession() as session:
        while True:
            await asyncio.sleep(1)
            async with session.get(base_url, params=params) as response:
                response.raise_for_status()
                result = await response.json()

                items = result.get('result', {}).get('items', [])
                if not items:
                    break

                for item in items:
                    posts.append(item['data'])
                    if posts_amount and len(posts) >= posts_amount:
                        return posts

                params['lastId'] = result.get('result', {}).get('lastId')
                params['lastSortingValue'] = result.get('result', {}).get('lastSortingValue')

                if not params['lastId']:
                    break

    return posts


async def fetch_tenchat_posts(username: str, posts_amount: Optional[int] = None):
    page = 0
    posts = []

    async with ClientSession() as session:
        while True:
            async with session.get(f'{TENCHAT_BASE_URL}/{username}?page={page}&size={TENCHAT_BASE_SIZE}') as response:
                response.raise_for_status()
                data = await response.json()

                content = data.get('content', [])
                if not content:
                    break

                for item in content:
                    posts.append(item)
                    if posts_amount and len(posts) >= posts_amount:
                        return posts

                if len(content) < TENCHAT_BASE_SIZE:
                    break

                page += 1
                await asyncio.sleep(1)

    return posts
