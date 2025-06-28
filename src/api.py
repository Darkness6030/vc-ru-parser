import asyncio
from typing import Optional, Dict, List, Union

from aiohttp import ClientSession
from bs4 import BeautifulSoup

TENCHAT_BASE_URL = 'https://tenchat.ru/gostinder/api/web/post/user/username'
TENCHAT_BASE_SIZE = 9


async def fetch_user_data(domain: str, **kwargs) -> Optional[Dict]:
    base_url = f'https://api.{domain}/v2.7/subsite'
    params = {'markdown': 'False', **kwargs}

    async with ClientSession() as session:
        async with session.get(base_url, params=params, timeout=None) as response:
            if not response.ok:
                return None

            result = await response.json()
            user_data = result['result']

            return {
                'url': user_data['url'],
                'name': user_data['name'],
                'is_blocked': user_data['robotsTag'] == 'noindex'
            }


async def fetch_tenchat_user_data(username_or_id: Union[str, int]) -> Optional[Dict]:
    async with ClientSession() as session:
        async with session.get(f'https://tenchat.ru/{username_or_id}', timeout=None, allow_redirects=True) as response:
            if not response.ok:
                return None

            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

            name_element = soup.find('h1', {'data-cy': 'name'})
            name = name_element.get_text(separator=' ', strip=True) if name_element else ''

            blocked_element = soup.find('div', {'data-cy': 'blocked'})
            is_blocked = blocked_element is not None

            return {
                'url': str(response.url),
                'name': name,
                'is_blocked': is_blocked
            }


async def fetch_user_posts(domain: str, user_id: int, posts_amount: Optional[int] = None) -> List[Dict]:
    base_url = f'https://api.{domain}/v2.8/timeline'
    params = {'markdown': 'false', 'sorting': 'new', 'subsitesIds': user_id}

    posts = []
    async with ClientSession() as session:
        while True:
            await asyncio.sleep(1)
            async with session.get(base_url, params=params, timeout=None) as response:
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


async def fetch_tenchat_posts(username: str, posts_amount: Optional[int] = None) -> List[Dict]:
    page = 0
    posts = []

    async with ClientSession() as session:
        while True:
            async with session.get(f'{TENCHAT_BASE_URL}/{username}?page={page}&size={TENCHAT_BASE_SIZE}', timeout=None) as response:
                response.raise_for_status()
                response_data = await response.json()

                content = response_data.get('content', [])
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
