import json
import os
import re
from datetime import datetime
from typing import Any
from typing import Optional, Tuple
from urllib.parse import urlparse, parse_qs, unquote

import pytz
from aiogram import Dispatcher, Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, FSInputFile, CallbackQuery, ForceReply
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import ClientSession
from rewire import simple_plugin

from src import sheets, api, bot
from src.callbacks import LoadModeCallback, CancelParsingCallback, ParseAllCallback, ParseAmountCallback
from src.states import UserState

plugin = simple_plugin()
router = Router()

OUTPUT_DIRECTORY = 'output'
LINK_TAG_PATTERN = r'<a\s+[^>]*?href=("(.*?)")[^>]*>'

menu_keyboard = InlineKeyboardBuilder() \
    .button(text='Выгрузить данные в JSON', callback_data=LoadModeCallback(mode='json')) \
    .button(text='Выгрузить данные в Google таблицы', callback_data=LoadModeCallback(mode='google')) \
    .adjust(1) \
    .as_markup()


async def parse_url(args: str) -> Optional[Tuple[str, str, Optional[int]]]:
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
        user_id = await api.fetch_user_id(domain, username)

    return domain, username, int(user_id)


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


@router.message(CommandStart())
async def start_command(message: Message):
    if not bot.is_admin(message.from_user.id):
        await message.answer('Нет доступа!')
        return

    await message.answer('Главное меню:', reply_markup=menu_keyboard)


@router.callback_query(LoadModeCallback.filter())
async def load_mode_callback(callback: CallbackQuery, callback_data: LoadModeCallback, state: FSMContext):
    await state.set_state(UserState.amount)
    await state.update_data(mode=callback_data.mode)
    await callback.message.answer(
        'Выберите количество постов:',
        reply_markup=InlineKeyboardBuilder()
        .button(text='Ввести количество', callback_data=ParseAmountCallback())
        .button(text='Все посты', callback_data=ParseAllCallback())
        .adjust(1)
        .as_markup()
    )


@router.callback_query(UserState.amount, ParseAmountCallback.filter())
async def parse_amount_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.amount_input)
    await callback.message.answer(
        'Введите количество постов:',
        reply_markup=ForceReply()
    )


@router.callback_query(UserState.amount, ParseAllCallback.filter())
async def parse_all_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.url)
    await state.update_data(amount=999999)
    await callback.message.answer(
        'Введите ссылку на пользователя:',
        reply_markup=ForceReply()
    )


@router.message(UserState.amount_input)
async def amount_handler(message: Message, state: FSMContext):
    amount = int(message.text) \
        if message.text.isdigit() \
        else 0

    if amount <= 0:
        await message.answer('Неверное количество постов. Попробуйте ещё раз:', reply_markup=ForceReply())
        return

    await state.set_state(UserState.url)
    await state.update_data(amount=amount)
    await message.answer(
        'Введите ссылку на пользователя:',
        reply_markup=ForceReply()
    )


@router.message(UserState.url)
async def url_handler(message: Message, state: FSMContext):
    match await state.get_value('mode'):
        case 'json':
            await load_json(message, state)

        case 'google':
            await load_google(message, state)


@router.callback_query(CancelParsingCallback.filter())
async def cancel_parsing_callback(callback: CallbackQuery, state: FSMContext):
    await state.update_data(cancelled=True)
    await callback.message.answer('Парсинг отменён.', reply_markup=menu_keyboard)


async def load_json(message: Message, state: FSMContext):
    parsed_args = await parse_url(message.text)
    if not parsed_args:
        await message.reply('Ошибка: Неверные аргументы или формат URL. Попробуйте ещё раз:', reply_markup=ForceReply())
        return

    amount = await state.get_value('amount')
    await state.clear()

    domain, username, user_id = parsed_args
    started_message = await message.answer(
        f'Начат парсинг постов для пользователя {username}...',
        reply_markup=InlineKeyboardBuilder()
        .button(text='Остановить', callback_data=CancelParsingCallback())
        .as_markup()
    )

    if domain == 'tenchat.ru':
        user_posts = await api.fetch_tenchat_posts(username, amount)
    else:
        user_posts = await api.fetch_user_posts(domain, user_id, amount or 999)

    if await state.get_value('cancelled'):
        await state.clear()
        return

    await started_message.edit_reply_markup()
    await message.answer(
        f'Получены данные {len(user_posts)} постов для пользователя {username}. Сохраняю на сервер...',
        reply_markup=InlineKeyboardBuilder()
        .button(text='Остановить', callback_data=CancelParsingCallback())
        .as_markup()
    )

    user_directory = os.path.join(OUTPUT_DIRECTORY, f'{domain.split(".")[0]}-{username}')
    async with ClientSession() as session:
        for post_data in user_posts:
            post_directory = os.path.join(user_directory, str(post_data['id']))
            os.makedirs(post_directory, exist_ok=True)

            if domain == 'tenchat.ru':
                pictures = post_data.get('pictures', [])
                for index, picture in enumerate(pictures):
                    link = picture.get('link')
                    if not link:
                        continue

                    if await state.get_value('cancelled'):
                        await state.clear()
                        return

                    async with session.get(link) as response:
                        if not response.ok:
                            continue

                        content_type = response.headers.get('Content-Type')
                        extension = content_type.split('/')[-1] if content_type else 'jpg'

                        image_path = os.path.join(post_directory, f"image_{index}.{extension}")
                        picture['path'] = image_path

                        with open(image_path, 'wb') as file:
                            file.write(await response.content.read())

            else:
                for block in post_data['blocks']:
                    if block['type'] == 'media':
                        for item in block['data']['items']:
                            if await state.get_value('cancelled'):
                                await state.clear()
                                return

                            image_data = item['image']['data']
                            url = f"https://leonardo.osnova.io/{image_data['uuid']}"

                            async with session.get(url) as response:
                                if not response.ok:
                                    continue

                                content_type = response.headers.get('Content-Type')
                                extension = content_type.split('/')[-1] if content_type else image_data['type']

                                image_path = os.path.join(post_directory, f"{image_data['uuid']}.{extension}")
                                image_data['path'] = image_path

                                with open(image_path, 'wb') as file:
                                    file.write(await response.content.read())

            post_json_path = os.path.join(post_directory, 'data.json')
            with open(post_json_path, 'w+') as post_file:
                json.dump(clean_json_links(post_data), post_file, ensure_ascii=False, indent=4)

    user_posts_path = os.path.join(user_directory, 'posts.json')
    with open(user_posts_path, 'w+') as user_posts_file:
        json.dump(clean_json_links(user_posts), user_posts_file, ensure_ascii=False, indent=4)

    await message.reply_document(
        FSInputFile(user_posts_path),
        caption=f'Все данные пользователя {username} успешно сохранены.',
        reply_markup=menu_keyboard
    )


async def load_google(message: Message, state: FSMContext):
    parsed_args = await parse_url(message.text)
    if not parsed_args:
        await message.reply('Ошибка: Неверные аргументы или формат URL. Попробуйте ещё раз:', reply_markup=ForceReply())
        return

    amount = await state.get_value('amount')
    await state.clear()

    domain, username, user_id = parsed_args
    started_message = await message.answer(
        f'Начат парсинг постов для пользователя {username}...',
        reply_markup=InlineKeyboardBuilder()
        .button(text='Остановить', callback_data=CancelParsingCallback())
        .as_markup()
    )

    if domain == 'tenchat.ru':
        user_posts = await api.fetch_tenchat_posts(username, amount)
    else:
        user_posts = await api.fetch_user_posts(domain, user_id, amount or 999)

    if await state.get_value('cancelled'):
        await state.clear()
        return

    await started_message.edit_reply_markup()
    await message.answer(f'Получены данные {len(user_posts)} постов. Сохраняю в Google таблицу...')

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
                'Автор': f'{post_data['user']['name'] or ''} {post_data['user']['surname'] or ''}'.strip(),
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

    sheets.update_user_data(
        title=f'{domain.split('.')[0][:3]}-{username}',
        rows=user_data
    )

    await message.reply(
        f'Все данные пользователя {username} успешно сохранены в Google таблицу.',
        reply_markup=menu_keyboard
    )


@plugin.setup()
def include_router(dispatcher: Dispatcher):
    dispatcher.include_router(router)
