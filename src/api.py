import asyncio
import time
from typing import Optional, Dict, List, Union

from aiohttp import ClientSession, BasicAuth
from bs4 import BeautifulSoup

from src import storage
from src.storage import TenchatAuthData

TENCHAT_BASE_URL = 'https://tenchat.ru/gostinder/api/web/post/user/username'
TENCHAT_BASE_SIZE = 9

TENCHAT_PROXY = 'http://eu.lunaproxy.com:12233'
TENCHAT_PROXY_AUTH = BasicAuth('user-reyingand_P0xoC-region-ru', '9QHJTXpnE07o')


async def fetch_tenchat_default_username(username: str) -> Optional[str]:
    auth_data = storage.get_tenchat_auth_data()
    if not auth_data:
        return None

    if auth_data.expires_at <= time.time():
        auth_data = await refresh_tenchat_auth_data(auth_data.refresh_token)
        storage.set_tenchat_auth_data(auth_data)

    user_url = f'https://tenchat.ru/gostinder/api/web/auth/account/username/{username}'
    headers = {
        'Authorization': f'Bearer {auth_data.access_token}'
    }

    try:
        async with ClientSession() as session:
            async with session.get(user_url, headers=headers, timeout=10) as response:
                response.raise_for_status()
                response_data = await response.json()
                return response_data.get('defaultUsername')
    except Exception as e:
        print(f'Ошибка при получении defaultUsername: {e}')
        return None


async def refresh_tenchat_auth_data(refresh_token: str) -> Optional[TenchatAuthData]:
    auth_url = 'https://tenchat.ru/vbc-oauth2-gostinder/oauth/token'
    payload = {
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token'
    }

    try:
        async with ClientSession() as session:
            async with session.post(auth_url, params=payload, timeout=10) as response:
                response.raise_for_status()
                response_data = await response.json()

                return TenchatAuthData(
                    access_token=response_data['access_token'],
                    refresh_token=response_data['refresh_token'],
                    expires_at=time.time() + response_data['expires_in']
                )
    except Exception as e:
        print(f'Ошибка при обновлении токена: {e}')
        return None


async def fetch_user_data(domain: str, username: str) -> Optional[Dict]:
    base_url = f'https://api.{domain}/v2.7/subsite'
    params = {'markdown': 'False', 'uri': username}

    async with ClientSession() as session:
        async with session.get(base_url, params=params, timeout=None) as response:
            if not response.ok:
                return None

            result = await response.json()
            user_data = result['result']

            return {
                'id': user_data['id'],
                'url': user_data['url'],
                'name': user_data['name'],
                'is_blocked': user_data['robotsTag'] == 'noindex'
            }


async def fetch_tenchat_user_data(username_or_id: Union[str, int]) -> Optional[Dict]:
    async with ClientSession() as session:
        async with session.get(
                f'https://tenchat.ru/{username_or_id}',
                timeout=None,
                allow_redirects=True,
                proxy=TENCHAT_PROXY,
                proxy_auth=TENCHAT_PROXY_AUTH
        ) as response:
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


async def fetch_user_posts(domain: str, username: str, posts_amount: Optional[int] = None) -> List[Dict]:
    base_url = f'https://api.{domain}/v2.8/timeline'
    params = {'markdown': 'false', 'sorting': 'new', 'uri': username}

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
            async with session.get(
                    f'{TENCHAT_BASE_URL}/{username}?page={page}&size={TENCHAT_BASE_SIZE}',
                    timeout=None,
                    proxy=TENCHAT_PROXY,
                    proxy_auth=TENCHAT_PROXY_AUTH
            ) as response:
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
