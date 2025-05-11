import asyncio

from aiohttp import ClientSession

TENCHAT_BASE_URL = 'https://tenchat.ru/gostinder/api/web/post/user/username'
TENCHAT_BASE_SIZE = 9


async def fetch_user_id(domain: str, user_url: str):
    base_url = f'https://api.{domain}/v2.7/subsite'
    params = {'markdown': 'False', 'uri': user_url}

    async with ClientSession() as session:
        async with session.get(base_url, params=params) as response:
            if not response.ok:
                return None

            result = await response.json()
            return result['result']['id']


async def fetch_user_posts(domain: str, user_id: int, posts_amount: int):
    base_url = f'https://api.{domain}/v2.8/timeline'
    params = {'markdown': 'false', 'sorting': 'new', 'subsitesIds': user_id}

    posts = []
    async with ClientSession() as session:
        while len(posts) < posts_amount:
            await asyncio.sleep(1)

            async with session.get(base_url, params=params) as response:
                response.raise_for_status()
                result = await response.json()

                items = result.get('result', {}).get('items', [])
                if not items:
                    break

                posts.extend([item['data'] for item in items])

                params['lastId'] = result.get('result', {}).get('lastId')
                params['lastSortingValue'] = result.get('result', {}).get('lastSortingValue')

    return posts[:posts_amount]


async def fetch_tenchat_posts(username: str, posts_amount):
    page = 0
    posts = []

    async with ClientSession() as session:
        while len(posts) < posts_amount:
            async with session.get(f'{TENCHAT_BASE_URL}/{username}?page={page}&size={TENCHAT_BASE_SIZE}') as response:
                response.raise_for_status()
                data = await response.json()

                content = data.get('content', [])
                if not content:
                    break

                for item in content:
                    posts.append(item)

            if len(content) < TENCHAT_BASE_SIZE:
                break

            page += 1
            await asyncio.sleep(1)

    return posts[:posts_amount]
