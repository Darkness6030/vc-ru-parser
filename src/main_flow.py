import asyncio
from typing import Match

from aiogram import Dispatcher, Router, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, FSInputFile, CallbackQuery, ForceReply, ReplyKeyboardRemove
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import ClientError
from rewire import simple_plugin

from src import api, bot, utils, storage
from src.callbacks import LoadModeCallback, CancelParsingCallback, ParseAllCallback, ParseAmountCallback, RegularParsingCallback, ParseNowCallback, TogglePauseCallback, ParseAccountCallback, AccountInfoCallback, AddAccountCallback, AccountsCallback, PeriodicityCallback, EditAccountCallback, DeleteAccountCallback, MainMenuCallback
from src.schedules import parse_account
from src.states import UserState

plugin = simple_plugin()
router = Router()

menu_keyboard = InlineKeyboardBuilder() \
    .button(text='Выгрузить данные в JSON', callback_data=LoadModeCallback(mode='json')) \
    .button(text='Выгрузить данные в Google таблицы', callback_data=LoadModeCallback(mode='google')) \
    .button(text='Парсинг по расписанию', callback_data=RegularParsingCallback()) \
    .adjust(1) \
    .as_markup()

regular_parsing_keyboard = InlineKeyboardBuilder() \
    .button(text='Назад', callback_data=RegularParsingCallback()) \
    .button(text='Назад в меню', callback_data=MainMenuCallback()) \
    .as_markup()

PARSING_MODES = ['табл', 'серв', 'оба']


@router.message(CommandStart())
async def start_command(message: Message, state: FSMContext):
    if not bot.is_admin(message.from_user.id):
        return await message.answer('⛔ Нет доступа!')

    await state.clear()
    await message.answer('📋 Главное меню:', reply_markup=menu_keyboard)


@router.callback_query(MainMenuCallback.filter())
async def main_menu_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer('📋 Главное меню:', reply_markup=menu_keyboard)


@router.callback_query(LoadModeCallback.filter())
async def load_mode_callback(callback: CallbackQuery, callback_data: LoadModeCallback, state: FSMContext):
    await state.set_state(UserState.amount_select)
    await state.update_data(mode=callback_data.mode)
    await callback.message.answer(
        'Выберите количество постов:',
        reply_markup=InlineKeyboardBuilder()
        .button(text='Ввести количество', callback_data=ParseAmountCallback())
        .button(text='Все посты', callback_data=ParseAllCallback())
        .adjust(1)
        .as_markup()
    )


@router.callback_query(UserState.amount_select, ParseAmountCallback.filter())
async def parse_amount_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.amount_input)
    await callback.message.answer(
        'Введите количество постов:',
        reply_markup=ForceReply()
    )


@router.callback_query(UserState.amount_select, ParseAllCallback.filter())
async def parse_all_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.url_select)
    await state.update_data(amount=None)
    await callback.message.answer(
        'Введите ссылку на пользователя:',
        reply_markup=ForceReply()
    )


@router.message(UserState.amount_input)
async def amount_handler(message: Message, state: FSMContext):
    amount = int(message.text) if message.text.isdigit() else 0

    if amount <= 0:
        return await message.answer('⚠️ Неверное количество постов. Попробуйте ещё раз:', reply_markup=ForceReply())

    await state.set_state(UserState.url_select)
    await state.update_data(amount=amount)
    await message.answer(
        'Введите ссылку на пользователя:',
        reply_markup=ForceReply()
    )


@router.message(UserState.url_select)
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


async def load_json(message: Message, state: FSMContext):
    parsed_args = await utils.parse_url(message.text)
    if not parsed_args:
        return await message.reply('❌ Ошибка: Неверные аргументы или формат URL. Попробуйте ещё раз:', reply_markup=ForceReply())

    amount = await state.get_value('amount')
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

    amount = await state.get_value('amount')
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


@router.callback_query(RegularParsingCallback.filter())
async def regular_parsing_callback(callback: CallbackQuery):
    is_paused = storage.is_paused()
    pause_status = '⏸️ Парсинг: пауза' if is_paused else '✅ Парсинг: работает'

    await callback.message.edit_text(
        '⚙️ Настройки регулярного парсинга:',
        reply_markup=InlineKeyboardBuilder()
        .button(text='👤 Кого парсим', callback_data=AccountsCallback())
        .button(text='⏰ Периодичность', callback_data=PeriodicityCallback())
        .button(text=pause_status, callback_data=TogglePauseCallback())
        .button(text='🔄 Спарсить сейчас', callback_data=ParseNowCallback(mode='menu'))
        .button(text='Назад в меню', callback_data=MainMenuCallback())
        .adjust(1)
        .as_markup()
    )


@router.callback_query(PeriodicityCallback.filter())
async def periodicity_callback(callback: CallbackQuery, state: FSMContext):
    periodicity = storage.get_periodicity()
    if periodicity:
        await state.set_state(UserState.periodicity_input)
        await callback.message.answer(
            'Текущая периодичность: '
            f'\nКаждые {periodicity.interval} дней, в {periodicity.time.strftime('%H:%M')} по Москве.\n'
            '\nВведите новую периодичность (Пример: 1 21:00):',
            reply_markup=ForceReply()
        )
    else:
        await state.set_state(UserState.periodicity_input)
        await callback.message.answer(
            'Периодичность пока не задана.'
            '\nВведите новую периодичность (Пример: 1 21:00):',
            reply_markup=ForceReply()
        )


@router.message(UserState.periodicity_input, F.text.regexp(r"^(\d+)\s+(\d{1,2}:\d{2})$").as_("match"))
async def periodicity_input(message: Message, state: FSMContext, match: Match[str]):
    interval = match.group(1)
    time_str = match.group(2)

    parsed_time = utils.parse_time(time_str)
    if not parsed_time:
        return await message.reply('⚠️ Неверный формат времени. Используйте HH:MM (например: 21:00)', reply_markup=ForceReply())

    storage.set_periodicity(
        interval=interval,
        time=parsed_time
    )

    await state.clear()
    await message.answer('✅ Периодичность обновлена!', reply_markup=regular_parsing_keyboard)


@router.callback_query(AccountsCallback.filter())
async def accounts_callback(callback: CallbackQuery):
    inline_keyboard = InlineKeyboardBuilder()
    inline_keyboard.button(text='➕ Добавить аккаунт', callback_data=AddAccountCallback())

    for account in storage.get_accounts():
        inline_keyboard.button(
            text=f'{account.domain.split('.')[0]} - {account.username}',
            callback_data=AccountInfoCallback(account_id=account.id)
        )

    await callback.message.edit_text(
        '👤 Кого парсим:',
        reply_markup=inline_keyboard
        .button(text='Назад', callback_data=RegularParsingCallback())
        .button(text='Назад в меню', callback_data=MainMenuCallback())
        .adjust(1)
        .as_markup()
    )


@router.callback_query(AddAccountCallback.filter())
async def add_account_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.add_account_input)
    await callback.message.answer(
        'Введите данные аккаунта в формате:\n'
        '<code>ссылка тип_парсинга (табл/серв/оба)</code>\n'
        'Пример: <code>https://dtf.ru/danny табл</code>',
        reply_markup=ForceReply()
    )


@router.message(UserState.add_account_input, F.text.regexp(r'^\S+\s+\S+$'))
async def add_account_input(message: Message, state: FSMContext):
    url, mode = message.text.split(maxsplit=2)
    if mode not in PARSING_MODES:
        return await message.reply(
            '⚠️ Неверный формат. Пример: <code>https://dtf.ru/danny табл</code>',
            reply_markup=ForceReply()
        )

    parsed_args = await utils.parse_url(url)
    if not parsed_args:
        return await message.reply(
            '❌ Ошибка: Неверные аргументы или формат URL. Попробуйте ещё раз:',
            reply_markup=ForceReply()
        )

    domain, username, user_id = parsed_args
    storage.add_account(
        url=url,
        mode=mode,
        domain=domain,
        username=username,
        user_id=user_id
    )

    await state.clear()
    await message.answer('✅ Аккаунт добавлен!', reply_markup=regular_parsing_keyboard)


@router.callback_query(AccountInfoCallback.filter())
async def account_info_callback(callback: CallbackQuery, callback_data: AccountInfoCallback):
    account = storage.get_account(callback_data.account_id)
    if not account:
        return

    await callback.message.edit_text(
        f'🔗 {account.url}'
        f'\nРежим: {account.mode}'
        '\nВыберите действие:',
        reply_markup=InlineKeyboardBuilder()
        .button(text='✏️ Редактировать', callback_data=EditAccountCallback(account_id=callback_data.account_id))
        .button(text='❌ Удалить', callback_data=DeleteAccountCallback(account_id=callback_data.account_id))
        .button(text='🔄 Спарсить сейчас', callback_data=ParseAccountCallback(account_id=callback_data.account_id))
        .button(text='Назад', callback_data=AccountsCallback())
        .adjust(1)
        .as_markup()
    )


@router.callback_query(EditAccountCallback.filter())
async def edit_account_callback(callback: CallbackQuery, callback_data: EditAccountCallback, state: FSMContext):
    account = storage.get_account(callback_data.account_id)
    if not account:
        return

    await state.set_state(UserState.edit_account_input)
    await state.update_data(account_id=callback_data.account_id)
    await callback.message.answer(
        'Введите новые данные в формате:\n'
        '<code>ссылка тип_парсинга (табл/серв/оба)</code>\n'
        'Пример: <code>https://dtf.ru/danny табл</code>',
        reply_markup=ForceReply()
    )


@router.message(UserState.edit_account_input, F.text.regexp(r'^\S+\s+\S+$'))
async def account_edit_input(message: Message, state: FSMContext):
    url, mode = message.text.split(maxsplit=2)
    if mode not in PARSING_MODES:
        return await message.reply(
            '⚠️ Неверный формат. Пример: <code>https://dtf.ru/danny табл</code>',
            reply_markup=ForceReply()
        )

    parsed_args = await utils.parse_url(url)
    if not parsed_args:
        return await message.reply(
            '❌ Ошибка: Неверные аргументы или формат URL. Попробуйте ещё раз:',
            reply_markup=ForceReply()
        )

    account_id = await state.get_value('account_id')
    domain, username, user_id = parsed_args
    storage.update_account(
        account_id=account_id,
        url=url,
        mode=mode,
        domain=domain,
        username=username,
        user_id=user_id
    )

    await state.clear()
    await message.answer('✅ Аккаунт обновлён!', reply_markup=regular_parsing_keyboard)


@router.callback_query(DeleteAccountCallback.filter())
async def delete_account_callback(callback: CallbackQuery, callback_data: DeleteAccountCallback):
    account = storage.get_account(callback_data.account_id)
    if not account:
        return

    storage.delete_account(callback_data.account_id)
    await callback.message.answer('✅ Аккаунт удалён!', reply_markup=regular_parsing_keyboard)


@router.callback_query(ParseAccountCallback.filter())
async def account_parse_callback(callback: CallbackQuery, callback_data: ParseAccountCallback):
    account = storage.get_account(callback_data.account_id)
    if not account:
        return await callback.message.answer('❌ Аккаунт не найден.')

    await callback.message.edit_text(f'🔄 Парсинг аккаунта {account.username}...')

    try:
        await parse_account(account)
        await callback.message.answer(f'✅ Парсинг завершён!\n{account.url}', reply_markup=regular_parsing_keyboard)
    except Exception as e:
        await callback.message.answer(f'❌ Ошибка парсинга:\n{str(e)}\n{account.url}', reply_markup=regular_parsing_keyboard, parse_mode=None)


@router.callback_query(TogglePauseCallback.filter())
async def toggle_pause_callback(callback: CallbackQuery):
    storage.toggle_pause()
    await regular_parsing_callback(callback)


@router.callback_query(ParseNowCallback.filter())
async def parse_now_callback(callback: CallbackQuery, callback_data: ParseNowCallback):
    if callback_data.mode == 'menu':
        return await callback.message.edit_text(
            'Выберите режим парсинга сейчас:',
            reply_markup=InlineKeyboardBuilder()
            .button(text='Оба', callback_data=ParseNowCallback(mode='оба'))
            .button(text='В таблицу', callback_data=ParseNowCallback(mode='табл'))
            .button(text='На сервер', callback_data=ParseNowCallback(mode='серв'))
            .button(text='Назад', callback_data=RegularParsingCallback())
            .button(text='Назад в меню', callback_data=MainMenuCallback())
            .adjust(1)
            .as_markup()
        )

    storage_data = storage.load_storage()
    await callback.message.edit_text(f'🔄 Запускаем парсинг {len(storage_data.accounts)} аккаунтов...')

    success_count = 0
    fail_count = 0

    async def safe_parse(account):
        nonlocal success_count, fail_count
        try:
            await parse_account(account, mode=callback_data.mode)
            success_count += 1
        except Exception:
            fail_count += 1

    tasks = [safe_parse(account) for account in storage_data.accounts]
    await asyncio.gather(*tasks)

    await callback.message.answer(
        f'✅ Парсинг завершён!\nУспешно: {success_count}\nОшибки: {fail_count}',
        reply_markup=regular_parsing_keyboard
    )


@plugin.setup()
def include_router(dispatcher: Dispatcher):
    dispatcher.include_router(router)
