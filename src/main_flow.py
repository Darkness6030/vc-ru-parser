import asyncio
from datetime import datetime
from datetime import timezone, timedelta
from typing import Match

from aiogram import Dispatcher, Router, F
from aiogram.filters import CommandStart, Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, FSInputFile, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import ClientError
from rewire import simple_plugin, logger

from src import api, bot, utils, storage, sheets
from src.callbacks import LoadModeCallback, CancelParsingCallback, ParseAllCallback, ParseAmountCallback, RegularParsingCallback, ParseNowCallback, RegularParsingToggleCallback, ParseAccountCallback, AccountInfoCallback, AddAccountCallback, AccountsCallback, RegularParsingPeriodicityCallback, EditAccountCallback, DeleteAccountCallback, MainMenuCallback, menu_keyboard, regular_parsing_keyboard, DeleteInvalidCallback, DeleteInvalidConfirmCallback, MonitorAccountsCallback, \
    MonitorPostsCallback, MonitorAccountsToggleCallback, MonitorAccountsToggleChangeURLCallback, MonitorAccountsToggleBlockingCallback, MonitorAccountsPeriodicityCallback, monitor_accounts_keyboard, MonitorAccountsSitesCallback, MonitorPostsPeriodicityCallback, MonitorPostsToggleCallback, MonitorPostsSitesCallback, monitor_posts_keyboard, MonitorPostsAccountsModeCallback, ParseBlockedConfirmCallback, ParseBlockedCancelCallback, ParseIDsCallback
from src.schedules import parse_account_posts
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


@router.message(UserState.url_select, F.text)
async def url_select_handler(message: Message, state: FSMContext):
    parsed_args = await utils.parse_url(message.text)
    if not parsed_args:
        return await message.answer('❌ Ошибка: Неверные аргументы или формат URL. Попробуйте ещё раз:')

    _, domain, username = parsed_args
    if not domain or not username:
        return await message.answer('❌ Ошибка: Пользователя не существует. Попробуйте ещё раз:')

    if domain == 'tenchat.ru':
        user_data = await api.fetch_tenchat_user_data(username)
        if user_data['is_blocked']:
            return await message.answer(
                '⚠️ Пользователь заблокирован. Всё равно парсим?',
                reply_markup=InlineKeyboardBuilder()
                .button(text='Да', callback_data=ParseBlockedConfirmCallback(mode=await state.get_value('mode'), username=username))
                .button(text='Нет', callback_data=ParseBlockedCancelCallback())
                .as_markup()
            )

    match await state.get_value('mode'):
        case 'server':
            await load_server(message, state, domain, username)

        case 'sheets':
            await load_sheets(message, state, domain, username)


@router.callback_query(ParseBlockedConfirmCallback.filter())
async def parse_blocked_confirm_callback(callback: CallbackQuery, state: FSMContext, callback_data: ParseBlockedConfirmCallback):
    match callback_data.mode:
        case 'server':
            await callback.message.delete()
            await load_server(callback.message, state, 'tenchat.ru', callback_data.username)

        case 'sheets':
            await callback.message.delete()
            await load_sheets(callback.message, state, 'tenchat.ru', callback_data.username)


@router.callback_query(ParseBlockedCancelCallback.filter())
async def parse_blocked_cancel_callback(callback: CallbackQuery, state: FSMContext):
    await main_menu_callback(callback, state)
    await callback.message.delete()


@router.callback_query(CancelParsingCallback.filter())
async def cancel_parsing_callback(callback: CallbackQuery, state: FSMContext):
    await state.update_data(cancelled=True)
    await callback.message.answer('🛑 Парсинг отменён.', reply_markup=menu_keyboard)


async def load_server(message: Message, state: FSMContext, domain: str, username: str):
    started_message = await message.answer(
        f'⏳ Начат парсинг постов для пользователя {username}...',
        reply_markup=InlineKeyboardBuilder()
        .button(text='Остановить', callback_data=CancelParsingCallback())
        .as_markup()
    )

    amount = await state.get_value('amount')
    await state.clear()

    try:
        if domain == 'tenchat.ru':
            user_posts = await api.fetch_tenchat_posts(username, amount)
        else:
            user_posts = await api.fetch_user_posts(domain, username, amount)
    except ClientError:
        await started_message.edit_text('⚠️ Ошибка при получении постов: пользователь не найден или произошёл сбой.')
        raise

    if await state.get_value('cancelled'):
        return await state.clear()

    await started_message.edit_reply_markup()
    await message.answer(f'📥 Получены данные {len(user_posts)} постов для пользователя {username}. Сохраняю на сервер...')

    user_posts_path = await utils.download_posts_files(domain, username, user_posts)
    document_message = await message.answer_document(FSInputFile(user_posts_path))
    await document_message.reply(
        f'✅ Все данные пользователя {username} успешно сохранены.',
        reply_markup=menu_keyboard
    )


async def load_sheets(message: Message, state: FSMContext, domain: str, username: str):
    started_message = await message.answer(
        f'⏳ Начат парсинг постов для пользователя {username}...',
        reply_markup=InlineKeyboardBuilder()
        .button(text='Остановить', callback_data=CancelParsingCallback())
        .as_markup()
    )

    amount = await state.get_value('amount')
    await state.clear()

    try:
        if domain == 'tenchat.ru':
            user_posts = await api.fetch_tenchat_posts(username, amount)
        else:
            user_posts = await api.fetch_user_posts(domain, username, amount)
    except ClientError:
        await started_message.edit_text('⚠️ Ошибка при получении постов: пользователь не найден или произошёл сбой.')
        raise

    if await state.get_value('cancelled'):
        return await state.clear()

    await started_message.edit_reply_markup()
    await message.answer(f'📤 Получены данные {len(user_posts)} постов. Сохраняю в Google таблицу...')

    await utils.unload_user_posts(domain, username, user_posts)
    await message.answer(
        f'✅ Все данные пользователя {username} успешно сохранены в Google таблицу.',
        reply_markup=menu_keyboard
    )


@router.callback_query(RegularParsingCallback.filter())
async def regular_parsing_callback(callback: CallbackQuery):
    storage_data = storage.load_storage()
    regular_parsing_status = '✅ Парсинг: работает' if storage_data.regular_parsing.enabled else '⏸️ Парсинг: пауза'

    await callback.message.edit_text(
        '⚙️ Настройки регулярного парсинга:',
        reply_markup=InlineKeyboardBuilder()
        .button(text='👤 Кого парсим', callback_data=AccountsCallback())
        .button(text='⏰ Периодичность', callback_data=RegularParsingPeriodicityCallback())
        .button(text=regular_parsing_status, callback_data=RegularParsingToggleCallback())
        .button(text='🔄 Спарсить сейчас', callback_data=ParseNowCallback(mode='menu'))
        .button(text='❌ Удалить невалид', callback_data=DeleteInvalidCallback())
        .button(text='📊 Мониторинг аккаунтов', callback_data=MonitorAccountsCallback())
        .button(text='📰 Мониторинг статей', callback_data=MonitorPostsCallback())
        .button(text='< Назад в меню', callback_data=MainMenuCallback())
        .adjust(1)
        .as_markup()
    )


@router.callback_query(RegularParsingToggleCallback.filter())
async def regular_parsing_toggle_callback(callback: CallbackQuery):
    storage.toggle_regular_parsing()
    await regular_parsing_callback(callback)


@router.callback_query(RegularParsingPeriodicityCallback.filter())
async def regular_parsing_periodicity_callback(callback: CallbackQuery, state: FSMContext):
    periodicity = storage.get_regular_parsing_periodicity()
    if periodicity:
        await state.set_state(UserState.regular_parsing_periodicity)
        await callback.message.answer(
            'Текущая периодичность: '
            f'\nКаждые {periodicity.interval} дней, в {periodicity.time.strftime('%H:%M')} по Москве.\n'
            '\nВведите новую периодичность (Пример: 1 21:00):'
        )
    else:
        await state.set_state(UserState.regular_parsing_periodicity)
        await callback.message.answer(
            'Периодичность пока не задана.'
            '\nВведите новую периодичность (Пример: 1 21:00):'
        )


@router.message(UserState.regular_parsing_periodicity, F.text.regexp(r"^(\d+)\s+(\d{1,2}:\d{2})$").as_("match"))
async def regular_parsing_periodicity_input(message: Message, state: FSMContext, match: Match[str]):
    interval = match.group(1)
    time_str = match.group(2)

    parsed_time = utils.parse_time(time_str)
    if not parsed_time:
        return await message.answer('⚠️ Неверный формат времени. Используйте HH:MM (например: 21:00)')

    storage.set_regular_parsing_periodicity(
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
    await state.set_state(UserState.add_account)
    await callback.message.answer(
        'Введите данные аккаунта в формате:\n'
        '<code>ссылка тип_парсинга (табл/серв/оба)</code>\n'
        'Пример: <code>https://dtf.ru/danny табл</code>'
    )


@router.message(UserState.add_account, F.text.regexp(r'^\S+\s+\S+$'))
async def add_account_input(message: Message, state: FSMContext):
    url, mode = message.text.split(maxsplit=2)
    if mode not in PARSING_MODES:
        return await message.answer(
            '⚠️ Неверный формат. Пример: <code>https://dtf.ru/danny табл</code>'
        )

    parsed_args = await utils.parse_url(url)
    if not parsed_args:
        return await message.answer(
            '❌ Ошибка: Неверные аргументы или формат URL. Попробуйте ещё раз:'
        )

    url, domain, username = parsed_args
    if not domain or not username:
        return await message.answer('❌ Ошибка: Пользователя не существует. Попробуйте ещё раз:')

    user_data = await api.fetch_tenchat_user_data(username) \
        if domain == 'tenchat.ru' else \
        await api.fetch_user_data(domain, username)

    # if user_data['is_blocked']:
    #     return await message.answer('❌ Ошибка: Пользователь заблокирован. Попробуйте ещё раз:')

    for account in storage.get_accounts():
        if account.url == url:
            return await message.answer('❌ Ошибка: Этот аккаунт уже добавлен. Попробуйте ещё раз:')

    storage.add_account(
        url=url,
        mode=mode,
        domain=domain,
        username=username,
        is_blocked=user_data['is_blocked']
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

    await state.set_state(UserState.edit_account)
    await state.update_data(account_id=callback_data.account_id)
    await callback.message.answer(
        'Введите новые данные в формате:\n'
        '<code>ссылка тип_парсинга (табл/серв/оба)</code>\n'
        'Пример: <code>https://dtf.ru/danny табл</code>'
    )


@router.message(UserState.edit_account, F.text.regexp(r'^\S+\s+\S+$'))
async def account_edit_input(message: Message, state: FSMContext):
    url, mode = message.text.split(maxsplit=2)
    if mode not in PARSING_MODES:
        return await message.answer(
            '⚠️ Неверный формат. Пример: <code>https://dtf.ru/danny табл</code>'
        )

    parsed_args = await utils.parse_url(url)
    if not parsed_args:
        return await message.answer(
            '❌ Ошибка: Неверные аргументы или формат URL. Попробуйте ещё раз:'
        )

    url, domain, username = parsed_args
    if not domain or not username:
        return await message.answer('❌ Ошибка: Пользователя не существует. Попробуйте ещё раз:')

    user_data = await api.fetch_tenchat_user_data(username) \
        if domain == 'tenchat.ru' else \
        await api.fetch_user_data(domain, username)

    # if user_data['is_blocked']:
    #     return await message.answer('❌ Ошибка: Пользователь заблокирован. Попробуйте ещё раз:')

    account_id = await state.get_value('account_id')
    storage.update_account(
        account_id=account_id,
        url=url,
        mode=mode,
        domain=domain,
        username=username,
        is_blocked=user_data['is_blocked']
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

    if not callback_data.skip_validation:
        user_data = await api.fetch_tenchat_user_data(account.username) \
            if account.domain == 'tenchat.ru' else \
            await api.fetch_user_data(account.domain, account.username)

        if user_data['is_blocked']:
            return await callback.message.answer(
                '⚠️ Пользователь заблокирован. Все равно парсим?',
                reply_markup=InlineKeyboardBuilder()
                .button(text='Да', callback_data=ParseAccountCallback(account_id=callback_data.account_id, skip_validation=True))
                .button(text='Нет', callback_data=ParseBlockedCancelCallback())
                .as_markup()
            )

    await callback.message.edit_text(f'🔄 Парсинг аккаунта {account.username}...')
    try:
        deleted_posts = await parse_account_posts(account, ignore_blocked=True)
        if deleted_posts:
            total_deleted = len(deleted_posts)
            logger.warning(f'Обнаружено {total_deleted} удалённых постов')

            lines = [
                '✅ Мониторинг Статей',
                f'Проверенных аккаунтов: 1',
                f'❌ Удаленных URL: {total_deleted}',
                f'\n{account.url} - {total_deleted}:'
            ]

            for post in deleted_posts:
                lines.append(f'{post['post_id']}')
            for post in deleted_posts:
                lines.append(f'{post['post_url']}')

            await bot.send_to_admins('\n'.join(lines))
            await sheets.update_monitor_posts_data(deleted_posts)

        await callback.message.answer(f'✅ Парсинг завершён!\n{account.url}', reply_markup=regular_parsing_keyboard)
    except Exception as e:
        await callback.message.answer(f'❌ Ошибка парсинга:\n{str(e)}\n{account.url}', reply_markup=regular_parsing_keyboard, parse_mode=None)


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

    accounts = storage.get_accounts()
    blocked_accounts = [account for account in accounts if account.is_blocked]
    active_accounts = [account for account in accounts if not account.is_blocked]

    if blocked_accounts and not callback_data.blocked_mode:
        return await callback.message.answer(
            '⚠️ Найдены заблокированные аккаунты. Какие парсим?',
            reply_markup=InlineKeyboardBuilder()
            .button(text='Парсим все', callback_data=ParseNowCallback(mode=callback_data.mode, blocked_mode='all'))
            .button(text='Только валидные', callback_data=ParseNowCallback(mode=callback_data.mode, blocked_mode='active'))
            .button(text='Только заблоченные', callback_data=ParseNowCallback(mode=callback_data.mode, blocked_mode='blocked'))
            .adjust(1)
            .as_markup()
        )

    if callback_data.blocked_mode == 'all':
        selected_accounts = accounts
    elif callback_data.blocked_mode == 'blocked':
        selected_accounts = blocked_accounts
    else:
        selected_accounts = active_accounts

    await callback.message.edit_text(f'🔄 Запускаем парсинг {len(selected_accounts)} аккаунтов...')
    success_count = 0
    failed_count = 0
    failed_accounts = []

    accounts_by_id = {account.id: account for account in accounts}
    all_deleted_posts = []
    grouped_deleted_posts = {}

    async def safe_parse(account):
        nonlocal success_count, failed_count
        try:
            deleted_posts = await parse_account_posts(account, ignore_blocked=True)
            if deleted_posts:
                all_deleted_posts.extend(deleted_posts)
                grouped_deleted_posts[account.id] = deleted_posts
            success_count += 1
        except Exception:
            failed_count += 1
            failed_accounts.append(account)

    tasks = [safe_parse(account) for account in selected_accounts]
    await asyncio.gather(*tasks)

    if grouped_deleted_posts:
        total_deleted = len(all_deleted_posts)
        logger.warning(f'Обнаружено {total_deleted} удалённых постов')

        lines = [
            '✅ Мониторинг Статей',
            f'Проверенных аккаунтов: {len(selected_accounts)}',
            f'Заблоченных аккаунтов: {len(blocked_accounts)}',
            f'❌ Удаленных URL: {total_deleted}'
        ]

        for account_id, posts in grouped_deleted_posts.items():
            account = accounts_by_id[account_id]
            lines.append(f'\n{account.url} - {len(posts)}:')
            for post in posts:
                lines.append(f'{post["post_id"]}')
            for post in posts:
                lines.append(f'{post["post_url"]}')

        await bot.send_to_admins('\n'.join(lines))

    result_lines = [
        f'✅ Парсинг завершён.',
        f'Всего аккаунтов: {len(selected_accounts)}',
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
        storage.add_last_failed_accounts(failed_accounts)

    await sheets.update_monitor_posts_data(all_deleted_posts)
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

    storage.clear_last_failed_accounts()
    await callback.message.edit_text(f'✅ Удалено {len(last_failed_accounts)} аккаунтов.', reply_markup=regular_parsing_keyboard)


@router.callback_query(MonitorAccountsCallback.filter())
async def monitor_accounts_callback(callback: CallbackQuery):
    accounts_settings = storage.get_monitor_accounts_settings()
    accounts_status = '✅ Мониторинг: ON' if accounts_settings.enabled else '❌ Мониторинг: OFF'
    change_url_status = '✅ Смена URL: ON' if accounts_settings.url_change_enabled else '❌ Смена URL: OFF'
    blocking_status = '✅ Блокировки: ON' if accounts_settings.blocking_enabled else '❌ Блокировки: OFF'

    if accounts_settings.last_run:
        msk_time = accounts_settings.last_run.astimezone(timezone(timedelta(hours=3)))
        formatted_time = msk_time.strftime('%d.%m.%Y %H:%M:%S')
    else:
        formatted_time = '—'

    await callback.message.edit_text(
        '⚙️ Настройки мониторинга аккаунтов:'
        f'\nПоследний запуск: {formatted_time}',
        reply_markup=InlineKeyboardBuilder()
        .button(text=accounts_status, callback_data=MonitorAccountsToggleCallback())
        .button(text='⏳ Периодичность', callback_data=MonitorAccountsPeriodicityCallback())
        .button(text=change_url_status, callback_data=MonitorAccountsToggleChangeURLCallback())
        .button(text=blocking_status, callback_data=MonitorAccountsToggleBlockingCallback())
        .button(text='🖥 Настройки сайтов', callback_data=MonitorAccountsSitesCallback())
        .button(text='Назад', callback_data=RegularParsingCallback())
        .adjust(1)
        .as_markup()
    )


@router.callback_query(MonitorAccountsToggleCallback.filter())
async def monitor_accounts_toggle_callback(callback: CallbackQuery):
    if storage.toggle_monitor_accounts():
        accounts_settings = storage.get_monitor_accounts_settings()
        accounts_settings.url_change_enabled = True
        accounts_settings.blocking_enabled = True
        storage.set_monitor_accounts_settings(accounts_settings)

    await monitor_accounts_callback(callback)


@router.callback_query(MonitorAccountsPeriodicityCallback.filter())
async def monitor_accounts_periodicity_callback(callback: CallbackQuery, state: FSMContext):
    accounts_settings = storage.get_monitor_accounts_settings()
    if accounts_settings.periodicity:
        await state.set_state(UserState.monitor_accounts_periodicity)
        await callback.message.answer(
            'Текущая периодичность: '
            f'\nКаждые {accounts_settings.periodicity} минут.\n'
            '\nВведите новую периодичность в минутах (Пример: 30):'
        )
    else:
        await state.set_state(UserState.monitor_accounts_periodicity)
        await callback.message.answer(
            'Периодичность пока не задана.'
            '\nВведите новую периодичность в минутах (Пример: 30):'
        )


@router.message(UserState.monitor_accounts_periodicity, F.text.isdigit())
async def monitor_accounts_periodicity_input(message: Message, state: FSMContext):
    accounts_settings = storage.get_monitor_accounts_settings()
    accounts_settings.periodicity = int(message.text)
    storage.set_monitor_accounts_settings(accounts_settings)

    await state.clear()
    await message.answer('✅ Периодичность обновлена!', reply_markup=monitor_accounts_keyboard)


@router.callback_query(MonitorAccountsToggleChangeURLCallback.filter())
async def monitor_accounts_toggle_change_url_callback(callback: CallbackQuery):
    accounts_settings = storage.get_monitor_accounts_settings()
    accounts_settings.url_change_enabled = not accounts_settings.url_change_enabled
    storage.set_monitor_accounts_settings(accounts_settings)
    await monitor_accounts_callback(callback)


@router.callback_query(MonitorAccountsToggleBlockingCallback.filter())
async def monitor_accounts_toggle_blocking_callback(callback: CallbackQuery):
    accounts_settings = storage.get_monitor_accounts_settings()
    accounts_settings.blocking_enabled = not accounts_settings.blocking_enabled
    storage.set_monitor_accounts_settings(accounts_settings)
    await monitor_accounts_callback(callback)


@router.callback_query(MonitorAccountsSitesCallback.filter())
async def monitor_accounts_sites_callback(callback: CallbackQuery, callback_data: MonitorAccountsSitesCallback):
    accounts_settings = storage.get_monitor_accounts_settings()
    if callback_data.toggle_dtf:
        accounts_settings.dtf_enabled = not accounts_settings.dtf_enabled
    elif callback_data.toggle_vc:
        accounts_settings.vc_enabled = not accounts_settings.vc_enabled
    elif callback_data.toggle_tenchat:
        accounts_settings.tenchat_enabled = not accounts_settings.tenchat_enabled

    storage.set_monitor_accounts_settings(accounts_settings)
    dtf_status = '✅ DTF: ON' if accounts_settings.dtf_enabled else '❌ DTF: OFF'
    vc_status = '✅ VC: ON' if accounts_settings.vc_enabled else '❌ VC: OFF'
    tenchat_status = '✅ TenChat: ON' if accounts_settings.tenchat_enabled else '❌ TenChat: OFF'

    await callback.message.edit_text(
        '⚙️ Настройки сайтов:',
        reply_markup=InlineKeyboardBuilder()
        .button(text=dtf_status, callback_data=MonitorAccountsSitesCallback(toggle_dtf=True))
        .button(text=vc_status, callback_data=MonitorAccountsSitesCallback(toggle_vc=True))
        .button(text=tenchat_status, callback_data=MonitorAccountsSitesCallback(toggle_tenchat=True))
        .button(text='Назад', callback_data=MonitorAccountsCallback())
        .adjust(1)
        .as_markup()
    )


@router.callback_query(MonitorPostsCallback.filter())
async def monitor_posts_callback(callback: CallbackQuery):
    posts_settings = storage.get_monitor_posts_settings()
    posts_status = '✅ Мониторинг: ON' if posts_settings.enabled else '❌ Мониторинг: OFF'

    if posts_settings.last_run:
        msk_time = posts_settings.last_run.astimezone(timezone(timedelta(hours=3)))
        formatted_time = msk_time.strftime('%d.%m.%Y %H:%M:%S')
    else:
        formatted_time = '—'

    await callback.message.edit_text(
        '⚙️ Настройки мониторинга постов:'
        f'\nПоследний запуск: {formatted_time}',
        reply_markup=InlineKeyboardBuilder()
        .button(text=posts_status, callback_data=MonitorPostsToggleCallback())
        .button(text='⏳ Периодичность', callback_data=MonitorPostsPeriodicityCallback())
        .button(text='🖇 Тип аккаунтов', callback_data=MonitorPostsAccountsModeCallback())
        .button(text='🖥 Настройки сайтов', callback_data=MonitorPostsSitesCallback())
        .button(text='Назад', callback_data=RegularParsingCallback())
        .adjust(1)
        .as_markup()
    )


@router.callback_query(MonitorPostsToggleCallback.filter())
async def monitor_posts_toggle_callback(callback: CallbackQuery):
    storage.toggle_monitor_posts()
    await monitor_posts_callback(callback)


@router.callback_query(MonitorPostsPeriodicityCallback.filter())
async def monitor_posts_periodicity_callback(callback: CallbackQuery, state: FSMContext):
    posts_settings = storage.get_monitor_posts_settings()
    formatted_periodicity = ', '.join(time.strftime('%H:%M') for time in posts_settings.periodicity) if posts_settings.periodicity else 'не задана'

    await state.set_state(UserState.monitor_posts_periodicity)
    await callback.message.answer(
        f'Текущая периодичность: {formatted_periodicity}'
        '\nВведите время в формате ЧЧ:ММ по Москве, по одному на строку:'
    )


@router.message(UserState.monitor_posts_periodicity, F.text)
async def monitor_posts_periodicity_input(message: Message, state: FSMContext):
    periodicity = []
    for line in message.text.splitlines():
        line = line.strip()
        if not line:
            continue

        try:
            parsed_time = datetime.strptime(line, '%H:%M').time()
            periodicity.append(parsed_time)
        except ValueError:
            await message.answer(f'❌ Неверный формат времени: `{line}`. Введите время в формате ЧЧ:ММ, по одному на строку.')
            return

    if not periodicity:
        await message.answer('❌ Не найдено ни одного корректного времени. Введите время в формате ЧЧ:ММ, по одному на строку.')
        return

    posts_settings = storage.get_monitor_posts_settings()
    posts_settings.periodicity = periodicity
    storage.set_monitor_posts_settings(posts_settings)

    await state.clear()
    await message.answer('✅ Периодичность обновлена!', reply_markup=monitor_posts_keyboard)


@router.callback_query(MonitorPostsAccountsModeCallback.filter())
async def monitor_posts_mode_callback(callback: CallbackQuery, callback_data: MonitorPostsAccountsModeCallback):
    posts_settings = storage.get_monitor_posts_settings()
    if posts_settings.accounts_mode == callback_data.accounts_mode:
        return

    if callback_data.accounts_mode:
        posts_settings.accounts_mode = callback_data.accounts_mode
        storage.set_monitor_posts_settings(posts_settings)

    await callback.message.edit_text(
        f'🖇 Текущий тип аккаунтов: {posts_settings.accounts_mode}',
        reply_markup=InlineKeyboardBuilder()
        .button(text='Все аккаунты', callback_data=MonitorPostsAccountsModeCallback(accounts_mode='все'))
        .button(text='Режим "Оба"', callback_data=MonitorPostsAccountsModeCallback(accounts_mode='оба'))
        .button(text='Режим "Табл"', callback_data=MonitorPostsAccountsModeCallback(accounts_mode='табл'))
        .button(text='Назад', callback_data=MonitorPostsCallback())
        .adjust(1)
        .as_markup()
    )


@router.callback_query(MonitorPostsSitesCallback.filter())
async def monitor_posts_sites_callback(callback: CallbackQuery, callback_data: MonitorPostsSitesCallback):
    posts_settings = storage.get_monitor_posts_settings()
    if callback_data.toggle_dtf:
        posts_settings.dtf_enabled = not posts_settings.dtf_enabled
    elif callback_data.toggle_vc:
        posts_settings.vc_enabled = not posts_settings.vc_enabled
    elif callback_data.toggle_tenchat:
        posts_settings.tenchat_enabled = not posts_settings.tenchat_enabled

    storage.set_monitor_posts_settings(posts_settings)
    dtf_status = '✅ DTF: ON' if posts_settings.dtf_enabled else '❌ DTF: OFF'
    vc_status = '✅ VC: ON' if posts_settings.vc_enabled else '❌ VC: OFF'
    tenchat_status = '✅ TenChat: ON' if posts_settings.tenchat_enabled else '❌ TenChat: OFF'

    await callback.message.edit_text(
        '⚙️ Настройки сайтов:',
        reply_markup=InlineKeyboardBuilder()
        .button(text=dtf_status, callback_data=MonitorPostsSitesCallback(toggle_dtf=True))
        .button(text=vc_status, callback_data=MonitorPostsSitesCallback(toggle_vc=True))
        .button(text=tenchat_status, callback_data=MonitorPostsSitesCallback(toggle_tenchat=True))
        .button(text='Назад', callback_data=MonitorPostsCallback())
        .adjust(1)
        .as_markup()
    )


@router.message(Command('tenchat_auth'))
async def tenchat_auth_command(message: Message, command: CommandObject):
    if not command.args:
        return await message.answer('⚠️ Использование: <code>/tenchat_auth <b>[refresh_token]</b></code>')

    auth_data = await api.refresh_tenchat_auth_data(command.args)
    if not auth_data:
        return await message.answer('⚠️ Не удалось получить данные авторизации. Скорее всего, передан неверный <b>refresh_token</b>!')

    storage.set_tenchat_auth_data(auth_data)
    await message.answer('✅ Данные авторизации установлены.')


@router.callback_query(ParseIDsCallback.filter())
async def parse_ids_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.username_links)
    await callback.message.answer('👤 Введи аккаунты для получения их ID:')


@router.message(UserState.username_links, F.text)
async def username_links_handler(message: Message, state: FSMContext):
    status_message = await message.answer('⏳ Обработка...')
    await state.clear()

    account_urls = message.text.splitlines()
    result_lines = []

    for account_url in account_urls:
        parsed_args = await utils.parse_url(account_url)
        if not parsed_args:
            result_lines.append(f'{account_url} — ❌')

        account_url, domain, username = parsed_args
        if domain == 'tenchat.ru':
            result_lines.append(account_url)
        else:
            user_data = await api.fetch_user_data(domain, username)
            result_lines.append(f'https://{domain}/id{user_data['id']}')

    result_text = '\n'.join(result_lines)
    await status_message.delete()
    await message.answer(
        f'👌 Результат обработки {len(account_urls)} ссылок:'
        f'\n\n{result_text}',
        reply_markup=InlineKeyboardBuilder()
        .button(text='Назад в меню', callback_data=MainMenuCallback())
        .as_markup()
    )


@plugin.setup()
def include_router(dispatcher: Dispatcher):
    dispatcher.include_router(router)
