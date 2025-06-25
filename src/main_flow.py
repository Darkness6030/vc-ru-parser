import asyncio
from typing import Match

from aiogram import Dispatcher, Router, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, FSInputFile, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import ClientError
from rewire import simple_plugin

from src import api, bot, utils, storage
from src.callbacks import LoadModeCallback, CancelParsingCallback, ParseAllCallback, ParseAmountCallback, RegularParsingCallback, ParseNowCallback, TogglePauseCallback, ParseAccountCallback, AccountInfoCallback, AddAccountCallback, AccountsCallback, PeriodicityCallback, EditAccountCallback, DeleteAccountCallback, MainMenuCallback, menu_keyboard, regular_parsing_keyboard, DeleteInvalidCallback, DeleteInvalidConfirmCallback
from src.schedules import parse_account
from src.states import UserState

plugin = simple_plugin()
router = Router()

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
        'Введите количество постов:'
    )


@router.callback_query(UserState.amount_select, ParseAllCallback.filter())
async def parse_all_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.url_select)
    await state.update_data(amount=None)
    await callback.message.answer(
        'Введите ссылку на пользователя:'
    )


@router.message(UserState.amount_input)
async def amount_handler(message: Message, state: FSMContext):
    amount = int(message.text) if message.text.isdigit() else 0

    if amount <= 0:
        return await message.answer('⚠️ Неверное количество постов. Попробуйте ещё раз:')

    await state.set_state(UserState.url_select)
    await state.update_data(amount=amount)
    await message.answer(
        'Введите ссылку на пользователя:'
    )


@router.message(UserState.url_select)
async def url_handler(message: Message, state: FSMContext):
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
        return await message.reply('❌ Ошибка: Неверные аргументы или формат URL. Попробуйте ещё раз:')

    domain, username, user_id = parsed_args
    if not domain or not username:
        return await message.reply('❌ Ошибка: Пользователя не существует. Попробуйте ещё раз:')

    if domain == 'tenchat.ru':
        if not await api.is_valid_tenchat_user(message.text):
            return await message.reply('❌ Ошибка: Пользователь заблокирован. Попробуйте ещё раз:')
    else:
        user_data = await api.fetch_user_data(domain, id=user_id)
        if user_data['name'] in ('Аккаунт удален', 'Удаленный аккаунт'):
            return await message.reply('❌ Ошибка: Пользователь заблокирован. Попробуйте ещё раз:')

    amount = await state.get_value('amount')
    await state.clear()

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
        return await message.reply('❌ Ошибка: Неверные аргументы или формат URL. Попробуйте ещё раз:')

    domain, username, user_id = parsed_args
    if not domain or not username:
        return await message.reply('❌ Ошибка: Пользователя не существует. Попробуйте ещё раз:')

    if domain == 'tenchat.ru':
        if not await api.is_valid_tenchat_user(message.text):
            return await message.reply('❌ Ошибка: Пользователь заблокирован. Попробуйте ещё раз:')
    else:
        user_data = await api.fetch_user_data(domain, id=user_id)
        if user_data['name'] in ('Аккаунт удален', 'Удаленный аккаунт'):
            return await message.reply('❌ Ошибка: Пользователь заблокирован. Попробуйте ещё раз:')

    amount = await state.get_value('amount')
    await state.clear()

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
        .button(text='❌ Удалить невалид', callback_data=DeleteInvalidCallback())
        .button(text='< Назад в меню', callback_data=MainMenuCallback())
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
            '\nВведите новую периодичность (Пример: 1 21:00):'
        )
    else:
        await state.set_state(UserState.periodicity_input)
        await callback.message.answer(
            'Периодичность пока не задана.'
            '\nВведите новую периодичность (Пример: 1 21:00):'
        )


@router.message(UserState.periodicity_input, F.text.regexp(r"^(\d+)\s+(\d{1,2}:\d{2})$").as_("match"))
async def periodicity_input(message: Message, state: FSMContext, match: Match[str]):
    interval = match.group(1)
    time_str = match.group(2)

    parsed_time = utils.parse_time(time_str)
    if not parsed_time:
        return await message.reply('⚠️ Неверный формат времени. Используйте HH:MM (например: 21:00)')

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
        'Пример: <code>https://dtf.ru/danny табл</code>'
    )


@router.message(UserState.add_account_input, F.text.regexp(r'^\S+\s+\S+$'))
async def add_account_input(message: Message, state: FSMContext):
    url, mode = message.text.split(maxsplit=2)
    if mode not in PARSING_MODES:
        return await message.reply(
            '⚠️ Неверный формат. Пример: <code>https://dtf.ru/danny табл</code>'
        )

    parsed_args = await utils.parse_url(url)
    if not parsed_args:
        return await message.reply(
            '❌ Ошибка: Неверные аргументы или формат URL. Попробуйте ещё раз:'
        )

    domain, username, user_id = parsed_args
    if not domain or not username:
        return await message.reply('❌ Ошибка: Пользователя не существует. Попробуйте ещё раз:')

    if domain == 'tenchat.ru':
        if not await api.is_valid_tenchat_user(url):
            return await message.reply('❌ Ошибка: Пользователь заблокирован. Попробуйте ещё раз:')
    else:
        user_data = await api.fetch_user_data(domain, id=user_id)
        if user_data['name'] in ('Аккаунт удален', 'Удаленный аккаунт'):
            return await message.reply('❌ Ошибка: Пользователь заблокирован. Попробуйте ещё раз:')

    for account in storage.get_accounts():
        if account.url == url:
            return await message.reply('❌ Ошибка: Этот аккаунт уже добавлен. Попробуйте ещё раз:')

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
        'Пример: <code>https://dtf.ru/danny табл</code>'
    )


@router.message(UserState.edit_account_input, F.text.regexp(r'^\S+\s+\S+$'))
async def account_edit_input(message: Message, state: FSMContext):
    url, mode = message.text.split(maxsplit=2)
    if mode not in PARSING_MODES:
        return await message.reply(
            '⚠️ Неверный формат. Пример: <code>https://dtf.ru/danny табл</code>'
        )

    parsed_args = await utils.parse_url(url)
    if not parsed_args:
        return await message.reply(
            '❌ Ошибка: Неверные аргументы или формат URL. Попробуйте ещё раз:'
        )

    domain, username, user_id = parsed_args
    if not domain or not username:
        return await message.reply('❌ Ошибка: Пользователя не существует. Попробуйте ещё раз:')

    if domain == 'tenchat.ru':
        if not await api.is_valid_tenchat_user(url):
            return await message.reply('❌ Ошибка: Пользователь заблокирован. Попробуйте ещё раз:')
    else:
        user_data = await api.fetch_user_data(domain, id=user_id)
        if user_data['name'] in ('Аккаунт удален', 'Удаленный аккаунт'):
            return await message.reply('❌ Ошибка: Пользователь заблокирован. Попробуйте ещё раз:')

    account_id = await state.get_value('account_id')
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

    if account.domain == 'tenchat.ru':
        if not await api.is_valid_tenchat_user(account.url):
            return await callback.message.answer('❌ Аккаунт заблокирован.')
    else:
        user_data = await api.fetch_user_data(account.domain, id=account.user_id)
        if user_data['name'] in ('Аккаунт удален', 'Удаленный аккаунт'):
            return await callback.message.answer('❌ Аккаунт заблокирован.')

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
            .button(text='По параметрам', callback_data=ParseNowCallback(mode=None))
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
    failed_count = 0
    failed_accounts = []

    async def safe_parse(account):
        nonlocal success_count, failed_count
        try:
            await parse_account(account, mode=callback_data.mode)
            success_count += 1
        except Exception:
            failed_count += 1
            failed_accounts.append(account)

    tasks = [safe_parse(account) for account in storage_data.accounts]
    await asyncio.gather(*tasks)

    result_lines = [
        f'✅ Парсинг завершён.',
        f'Всего аккаунтов: {len(storage_data.accounts)}',
        f'Успешно: {success_count}',
        f'Неуспешно: {failed_count}'
    ]

    inline_keyboard = InlineKeyboardBuilder() \
        .button(text='Назад', callback_data=RegularParsingCallback()) \
        .button(text='Назад в меню', callback_data=MainMenuCallback())

    if failed_accounts:
        result_lines.append('\n❗️Не удалось спарсить следующие аккаунты:')
        for index, account in enumerate(failed_accounts, start=1):
            result_lines.append(f'{account.url} ({account.name or account.username})')

        inline_keyboard.button(text='❌ Удалить невалид', callback_data=DeleteInvalidCallback())
        storage.set_last_failed_accounts(failed_accounts)

    await callback.message.answer(
        '\n'.join(result_lines),
        reply_markup=inline_keyboard.adjust(2).as_markup()
    )


@router.callback_query(DeleteInvalidCallback.filter())
async def delete_invalid_callback(callback: CallbackQuery):
    last_failed_accounts = storage.get_last_failed_accounts()
    if not last_failed_accounts:
        return await callback.answer('Нет аккаунтов для удаления.')

    accounts_message = 'Не удалось спарсить следующие аккаунты:\n\n'
    for index, account in enumerate(last_failed_accounts, start=1):
        accounts_message += f'{account.url} ({account.name or account.username})\n'

    accounts_message += '\nУдалить эти аккаунты?'
    await callback.message.edit_text(
        accounts_message,
        reply_markup=InlineKeyboardBuilder()
        .button(text='Да', callback_data=DeleteInvalidConfirmCallback(confirm=True))
        .button(text='Нет', callback_data=DeleteInvalidConfirmCallback(confirm=False))
        .as_markup()
    )


@router.callback_query(DeleteInvalidConfirmCallback.filter())
async def delete_invalid_confirm_callback(callback: CallbackQuery, callback_data: DeleteInvalidConfirmCallback):
    if not callback_data.confirm:
        return await callback.message.edit_text('❌ Ок, отменяем.', reply_markup=regular_parsing_keyboard)

    last_failed_accounts = storage.get_last_failed_accounts()
    for account in last_failed_accounts:
        storage.delete_account(account.id)

    storage.set_last_failed_accounts([])
    await callback.message.edit_text(f'✅ Удалено {len(last_failed_accounts)} аккаунтов.', reply_markup=regular_parsing_keyboard)


@plugin.setup()
def include_router(dispatcher: Dispatcher):
    dispatcher.include_router(router)
