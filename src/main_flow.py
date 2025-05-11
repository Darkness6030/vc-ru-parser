from aiogram import Dispatcher, Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, FSInputFile, CallbackQuery, ForceReply, ReplyKeyboardRemove
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import ClientError
from rewire import simple_plugin

from src import api, bot, utils
from src.callbacks import LoadModeCallback, CancelParsingCallback, ParseAllCallback, ParseAmountCallback, RegularParsingCallback
from src.states import UserState

plugin = simple_plugin()
router = Router()

menu_keyboard = InlineKeyboardBuilder() \
    .button(text='Выгрузить данные в JSON', callback_data=LoadModeCallback(mode='json')) \
    .button(text='Выгрузить данные в Google таблицы', callback_data=LoadModeCallback(mode='google')) \
    .button(text='Парсинг по расписанию', callback_data=RegularParsingCallback()) \
    .adjust(1) \
    .as_markup()


@router.message(CommandStart())
async def start_command(message: Message):
    if not bot.is_admin(message.from_user.id):
        return await message.answer('⛔ Нет доступа!')

    await message.answer('📋 Главное меню:', reply_markup=menu_keyboard)


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
    amount = int(message.text) if message.text.isdigit() else 0

    if amount <= 0:
        return await message.answer('⚠️ Неверное количество постов. Попробуйте ещё раз:', reply_markup=ForceReply())

    await state.set_state(UserState.url)
    await state.update_data(amount=amount)
    await message.answer(
        'Введите ссылку на пользователя:',
        reply_markup=ForceReply()
    )


@router.message(UserState.url)
async def url_handler(message: Message, state: FSMContext):
    loading_message = await message.answer('Загрузка...', reply_markup=ReplyKeyboardRemove())
    await loading_message.delete()

    match await state.get_value('mode'):
        case 'json':
            await load_json(message, state)

        case 'google':
            await load_google(message, state)


@router.callback_query(CancelParsingCallback.filter())
async def cancel_parsing_callback(callback: CallbackQuery, state: FSMContext):
    await state.update_data(cancelled=True)
    await callback.message.answer('🛑 Парсинг отменён.', reply_markup=menu_keyboard)


@router.callback_query(RegularParsingCallback.filter())
async def regular_parsing_callback(callback: CallbackQuery, state: FSMContext):
    pass


async def load_json(message: Message, state: FSMContext):
    parsed_args = await utils.parse_url(message.text)
    if not parsed_args:
        return await message.reply('❌ Ошибка: Неверные аргументы или формат URL. Попробуйте ещё раз:', reply_markup=ForceReply())

    amount = await state.get_value('amount', 999999)
    await state.clear()

    domain, username, user_id = parsed_args
    started_message = await message.reply(
        f'⏳ Начат парсинг постов для пользователя {username}...',
        reply_markup=InlineKeyboardBuilder()
        .button(text='Остановить', callback_data=CancelParsingCallback())
        .as_markup()
    )

    try:
        if domain == 'tenchat.ru':
            user_posts = await api.fetch_tenchat_posts(username, amount)
        else:
            user_posts = await api.fetch_user_posts(domain, user_id, amount)
    except ClientError:
        return await started_message.edit_text('⚠️ Ошибка при получении постов: пользователь не найден или произошёл сбой.')

    if await state.get_value('cancelled'):
        return await state.clear()

    await started_message.edit_reply_markup()
    await message.answer(
        f'📥 Получены данные {len(user_posts)} постов для пользователя {username}. Сохраняю на сервер...',
        reply_markup=InlineKeyboardBuilder()
        .button(text='Остановить', callback_data=CancelParsingCallback())
        .as_markup()
    )

    user_posts_path = await utils.download_posts_files(domain, username, user_posts)
    await message.reply_document(
        FSInputFile(user_posts_path),
        caption=f'✅ Все данные пользователя {username} успешно сохранены.',
        reply_markup=menu_keyboard
    )


async def load_google(message: Message, state: FSMContext):
    parsed_args = await utils.parse_url(message.text)
    if not parsed_args:
        return await message.reply('❌ Ошибка: Неверные аргументы или формат URL. Попробуйте ещё раз:', reply_markup=ForceReply())

    amount = await state.get_value('amount', 999999)
    await state.clear()

    domain, username, user_id = parsed_args
    started_message = await message.reply(
        f'⏳ Начат парсинг постов для пользователя {username}...',
        reply_markup=InlineKeyboardBuilder()
        .button(text='Остановить', callback_data=CancelParsingCallback())
        .as_markup()
    )

    try:
        if domain == 'tenchat.ru':
            user_posts = await api.fetch_tenchat_posts(username, amount)
        else:
            user_posts = await api.fetch_user_posts(domain, user_id, amount)
    except ClientError:
        return await started_message.edit_text('⚠️ Ошибка при получении постов: пользователь не найден или произошёл сбой.')

    if await state.get_value('cancelled'):
        return await state.clear()

    await started_message.edit_reply_markup()
    await message.answer(f'📤 Получены данные {len(user_posts)} постов. Сохраняю в Google таблицу...')

    await utils.unload_posts_to_sheets(domain, username, user_posts)
    await message.reply(
        f'✅ Все данные пользователя {username} успешно сохранены в Google таблицу.',
        reply_markup=menu_keyboard
    )


@plugin.setup()
def include_router(dispatcher: Dispatcher):
    dispatcher.include_router(router)
