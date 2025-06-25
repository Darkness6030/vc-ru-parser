import json
import os
import re
from datetime import datetime, date
from typing import Any, Optional, Tuple
from urllib.parse import unquote, urlparse, parse_qs

import pytz
from aiohttp import ClientSession

from src import sheets, api

OUTPUT_DIRECTORY = 'output'
LINK_TAG_PATTERN = r'<a\s+[^>]*?href=("(.*?)")[^>]*>'


def parse_time(text: str):
    try:
        return datetime.strptime(text, '%H:%M').time()
    except ValueError:
        return None


async def parse_url(args: str) -> Optional[Tuple[Optional[str], Optional[str], Optional[int]]]:
    parsed = urlparse(args)
    domain = parsed.netloc.lower()
    path = parsed.path.strip('/')

    if not domain or not path:
        return None

    if domain == 'tenchat.ru':
        return domain, path, None

    match = re.match(r'(id(\d+))|u/(\d+)-([\w\-]+)|([\w\-]+)', path)
    if not match:
        return None

    user_id = match.group(2) or match.group(3)
    username = match.group(4) or match.group(5) or f'id{user_id}'

    if not user_id:
        user_data = await api.fetch_user_data(domain, uri=username)
        if not user_data:
            return None, None, None

        user_id = user_data['id']

    return domain, username, int(user_id) if user_id else None


def replace_redirect_links(href: str) -> str:
    if 'redirect?to=' in href:
        parsed_url = urlparse(href)
        query_params = parse_qs(parsed_url.query)
        if 'to' in query_params:
            return unquote(query_params['to'][0])
    return href


def clean_links_in_text(text: str) -> str:
    if re.fullmatch(r'https?://[^\s]+', text):
        return replace_redirect_links(text)

    def href_replacer(match):
        original_href = match.group(1) or match.group(2)
        clean_href = replace_redirect_links(original_href)
        return f'<a href="{clean_href}">' if match.group(1) else clean_href

    return re.sub(LINK_TAG_PATTERN, href_replacer, text)


def clean_json_links(data: Any) -> Any:
    if isinstance(data, dict):
        return {key: clean_json_links(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [clean_json_links(item) for item in data]
    elif isinstance(data, str):
        return clean_links_in_text(data)
    return data


async def download_posts_files(domain: str, username: str, user_posts: list, last_post_id: Optional[int] = None):
    user_directory = os.path.join(OUTPUT_DIRECTORY, f'{domain.split('.')[0]}-{username}')
    os.makedirs(user_directory, exist_ok=True)

    async with ClientSession() as session:
        for post_data in user_posts:
            if last_post_id and post_data['id'] <= last_post_id:
                continue

            post_directory = os.path.join(user_directory, str(post_data['id']))
            os.makedirs(post_directory, exist_ok=True)

            if domain == 'tenchat.ru':
                pictures = post_data.get('pictures', [])
                for index, picture in enumerate(pictures):
                    link = picture.get('link')
                    if not link:
                        continue

                    async with session.get(link) as response:
                        if not response.ok:
                            continue

                        content_type = response.headers.get('Content-Type')
                        extension = content_type.split('/')[-1] if content_type else 'jpg'

                        image_path = os.path.join(post_directory, f'image_{index}.{extension}')
                        picture['path'] = image_path

                        with open(image_path, 'wb') as file:
                            file.write(await response.content.read())

            else:
                for block in post_data['blocks']:
                    if block['type'] == 'media':
                        for item in block['data']['items']:
                            image_data = item['image']['data']
                            url = f'https://leonardo.osnova.io/{image_data['uuid']}'

                            async with session.get(url) as response:
                                if not response.ok:
                                    continue

                                content_type = response.headers.get('Content-Type')
                                extension = content_type.split('/')[-1] if content_type else image_data['type']

                                image_path = os.path.join(post_directory, f'{image_data['uuid']}.{extension}')
                                image_data['path'] = image_path

                                with open(image_path, 'wb') as file:
                                    file.write(await response.content.read())

            post_json_path = os.path.join(post_directory, 'data.json')
            with open(post_json_path, 'w+') as post_file:
                json.dump(clean_json_links(post_data), post_file, ensure_ascii=False, indent=4)

    user_posts_path = os.path.join(user_directory, 'posts.json')
    with open(user_posts_path, 'w+') as user_posts_file:
        json.dump(clean_json_links(user_posts), user_posts_file, ensure_ascii=False, indent=4)

    return user_posts_path


async def unload_posts_to_sheets(domain: str, username: str, user_posts: list):
    user_data = []
    for post_data in user_posts:
        date_now = datetime.now(pytz.timezone('Europe/Moscow'))

        if domain == 'tenchat.ru':
            date_published = datetime.fromisoformat(post_data['publishDate'])
            user_data.append({
                'ID': post_data['id'],
                'URL': f'https://tenchat.ru/media/{post_data['titleTransliteration']}',
                'Название статьи': post_data['title'],
                'Просмотры': post_data['viewCount'],
                'Добавлено': date_published.strftime('%Y-%m-%d %H:%M:%S'),
                'Автор': f"{post_data['user']['name'] or ''} {post_data['user']['surname'] or ''}".strip(),
                'Парсинг': date_now.strftime('%Y-%m-%d %H:%M:%S')
            })
        else:
            date_published = datetime.fromtimestamp(post_data['date'], pytz.timezone('Europe/Moscow'))
            user_data.append({
                'ID': post_data.get('id'),
                'URL': post_data.get('url'),
                'Название статьи': post_data['title'],
                'Просмотры': post_data['counters']['hits'],
                'Добавлено': date_published.strftime('%Y-%m-%d %H:%M:%S'),
                'Автор': post_data['author']['name'],
                'Парсинг': date_now.strftime('%Y-%m-%d %H:%M:%S')
            })

    await sheets.update_user_data(
        title=f'{domain.split('.')[0][:3]}-{username}',
        rows=user_data
    )


def extract_user_data(domain: str, username: str, user_posts: list[dict]) -> dict:
    name = user_posts[0]['author']['name']

    today_posts = 0
    today_views = 0
    total_posts = 0
    total_views = 0

    for post in user_posts:
        post_date = datetime.fromtimestamp(post['date']).date()
        views = post['counters']['hits']
        total_posts += 1
        total_views += views

        if post_date == date.today():
            today_posts += 1
            today_views += views

    return {
        'url': f'https://{domain}/{username}',
        'name': name,
        'today_posts': today_posts,
        'today_views': today_views,
        'total_posts': total_posts,
        'total_views': total_views,
    }


def extract_tenchat_user_data(username: str, user_posts: list[dict]) -> dict:
    user = user_posts[0]['user']
    name = user.get('name', '')
    surname = user.get('surname', '')

    today_posts = 0
    today_views = 0
    total_posts = 0
    total_views = 0

    for post in user_posts:
        publish_date = datetime.fromisoformat(post['publishDate'].rstrip('Z')).date()
        views = post['viewCount']
        total_posts += 1
        total_views += views

        if publish_date == date.today():
            today_posts += 1
            today_views += views

    return {
        'url': f'https://tenchat.ru/{username}',
        'name': f'{name} {surname}'.strip(),
        'surname': surname,
        'today_posts': today_posts,
        'today_views': today_views,
        'total_posts': total_posts,
        'total_views': total_views,
    }
