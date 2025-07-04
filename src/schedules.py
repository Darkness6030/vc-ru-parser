import asyncio
from datetime import datetime
from typing import Optional

import pytz
from aiogram.utils.keyboard import InlineKeyboardBuilder
from rewire import simple_plugin, logger

from src import storage, utils, api, sheets, bot
from src.callbacks import RegularParsingCallback, MainMenuCallback, DeleteInvalidCallback
from src.storage import Account, RegularParsingSettings, MonitorAccountsSettings, MonitorPostsSettings

plugin = simple_plugin()

SEMAPHORE = asyncio.Semaphore(1)
MOSCOW_TIMEZONE = pytz.timezone('Europe/Moscow')


async def parse_account_posts(account: Account, mode: Optional[str] = None, ignore_blocked: bool = False):
    async with SEMAPHORE:
        domain, username = account.domain, account.username
        user_data = await api.fetch_tenchat_user_data(username) \
            if domain == 'tenchat.ru' else \
            await api.fetch_user_data(domain, username)

        if 'name' in user_data:
            account.name = user_data['name']
            storage.update_account(account.id, url=account.url, name=account.name)

        if not ignore_blocked and user_data['is_blocked']:
            logger.error(f'Аккаунт {username} заблокирован')
            raise

        try:
            logger.info(f'Получаем посты для {username}...')
            if domain == 'tenchat.ru':
                user_posts = await api.fetch_tenchat_posts(username)
            else:
                user_posts = await api.fetch_user_posts(domain, username)
        except Exception as e:
            logger.error(f'Ошибка при получении постов для {username}: {e}', exc_info=True)
            raise

        logger.info(f'Получены {len(user_posts)} постов для {username}')
        deleted_posts = []

        if account.mode == 'оба' and not account.is_blocked:
            try:
                existing_posts = await utils.load_user_posts(domain, username)
                monitor_posts_ids = await sheets.get_monitor_posts_ids()

                parsed_ids = {post['id'] for post in user_posts}
                deleted_posts = [
                    {
                        'account_url': account.url,
                        'name': account.name or account.username,
                        **post
                    }
                    for post in existing_posts if post['post_id'] not in parsed_ids and post['post_id'] not in monitor_posts_ids
                ]
            except Exception as e:
                logger.error(f'Ошибка при мониторинге постов для {username}: {e}', exc_info=True)

        try:
            mode = mode or account.mode
            if mode in ('серв', 'оба'):
                await utils.download_posts_files(domain, username, user_posts, last_post_id=account.last_post_id)

                last_post_id = user_posts[0]['id']
                storage.update_account(account.id, last_post_id=last_post_id)

                logger.info(f'Файлы {username} сохранены на сервер')

            if mode in ('табл', 'оба'):
                user_data = utils.extract_tenchat_user_data(username, user_posts) \
                    if domain == 'tenchat.ru' else \
                    utils.extract_user_data(domain, username, user_posts)

                await sheets.update_regular_parsing_data([user_data])
                await utils.unload_user_posts(domain, username, user_posts)

                logger.info(f'Данные {username} выгружены в Google таблицу')
        except Exception as e:
            logger.error(f'Ошибка при обработке данных для {username}: {e}', exc_info=True)
            raise

        return deleted_posts


def should_regular_parsing_run(settings: RegularParsingSettings) -> bool:
    if not settings.enabled or not settings.periodicity:
        return False

    now = datetime.now(MOSCOW_TIMEZONE)
    if settings.last_run:
        last_run = settings.last_run.astimezone(MOSCOW_TIMEZONE)
        days_passed = (now.date() - last_run.date()).days

        if days_passed < settings.periodicity.interval:
            return False

    target_time = datetime.combine(now.date(), settings.periodicity.time)
    target_time = MOSCOW_TIMEZONE.localize(target_time)

    return now >= target_time


def should_monitor_accounts_run(settings: MonitorAccountsSettings) -> bool:
    if not settings.enabled or not settings.periodicity:
        return False

    now = datetime.now(MOSCOW_TIMEZONE)
    if settings.last_run:
        last_run = settings.last_run.astimezone(MOSCOW_TIMEZONE)
        minutes_passed = (now - last_run).total_seconds() / 60

        if minutes_passed <= settings.periodicity:
            return False

    return True


def should_monitor_posts_run(settings: MonitorPostsSettings) -> bool:
    if not settings.enabled or not settings.periodicity:
        return False

    now = datetime.now(MOSCOW_TIMEZONE)
    today = now.date()

    for target_time in settings.periodicity:
        target_datetime = MOSCOW_TIMEZONE.localize(datetime.combine(today, target_time))
        time_difference = abs((target_datetime - now).total_seconds())

        if time_difference <= 60:
            return True

    return False


async def schedule_regular_parsing_runner():
    while True:
        try:
            regular_parsing_settings = storage.get_regular_parsing_settings()
            if not regular_parsing_settings.enabled or not should_regular_parsing_run(regular_parsing_settings):
                await asyncio.sleep(10)
                continue

            accounts = storage.get_accounts()
            blocked_accounts = [account for account in accounts if account.is_blocked]
            active_accounts = [account for account in accounts if not account.is_blocked]

            logger.info(f'🚀 Начат плановый парсинг {len(active_accounts)} аккаунтов...')
            storage.update_regular_parsing_last_run()

            success_count = 0
            failed_count = 0
            failed_accounts = []

            accounts_by_id = {account.id: account for account in accounts}
            all_deleted_posts = []
            grouped_deleted_posts = {}

            async def safe_parse(account):
                nonlocal success_count, failed_count
                try:
                    deleted_posts = await parse_account_posts(account)
                    if deleted_posts:
                        all_deleted_posts.extend(deleted_posts)
                        grouped_deleted_posts[account.id] = deleted_posts
                    success_count += 1
                except Exception:
                    failed_count += 1
                    failed_accounts.append(account)

            tasks = [safe_parse(account) for account in active_accounts]
            await asyncio.gather(*tasks)

            if grouped_deleted_posts:
                total_deleted = len(all_deleted_posts)
                logger.warning(f'Обнаружено {total_deleted} удалённых постов')

                lines = [
                    '✅ Мониторинг Статей',
                    f'Проверенных аккаунтов: {len(active_accounts)}',
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
                f'Всего аккаунтов: {len(storage.get_accounts())}',
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
            await bot.send_to_admins(
                '\n'.join(result_lines),
                reply_markup=inline_keyboard.adjust(2).as_markup()
            )

            logger.info(f'✅ Парсинг завершён. Успешно: {success_count}, Неуспешно: {failed_count}.')
            logger.info(f'⏳ Следующий запуск через {regular_parsing_settings.periodicity.interval} дней.')
        except Exception as e:
            logger.exception(f'Ошибка в планировщике: {e}', exc_info=True)
            await asyncio.sleep(1)


async def schedule_monitor_accounts_runner():
    while True:
        try:
            monitor_accounts_settings = storage.get_monitor_accounts_settings()
            if not should_monitor_accounts_run(monitor_accounts_settings):
                await asyncio.sleep(10)
                continue

            logger.info('🔄 Запуск мониторинга аккаунтов...')
            accounts = [account for account in storage.get_accounts()]
            storage.update_monitor_accounts_last_run()

            changed_accounts = []
            blocked_accounts = []
            url_changed_accounts = []

            for account in accounts:
                domain, username = account.domain, account.username
                logger.debug(f'Проверка аккаунта {username} ({domain})')

                try:
                    user_data = await api.fetch_tenchat_user_data(username) \
                        if domain == 'tenchat.ru' else \
                        await api.fetch_user_data(domain, username)

                    assert user_data
                except Exception as e:
                    logger.error(f'Ошибка при получении данных для {account.username}: {e}', exc_info=True)
                    continue

                url_changed = user_data['url'] != account.last_url
                blocked_changed = user_data['is_blocked'] != account.is_blocked

                if url_changed:
                    status = 'смена URL' if account.last_url else 'перв.монит'
                    logger.info(f'Обнаружено изменение URL для {username}: {account.last_url} -> {user_data['url']}')
                    if account.last_url:
                        url_changed_accounts.append({
                            'user_url': account.url,
                            'old_url': account.last_url,
                            'new_url': user_data['url'],
                        })

                    account.name = user_data['name']
                    account.last_url = user_data['url']
                    storage.update_account(account.id, name=account.name, last_url=account.last_url)
                elif blocked_changed:
                    status = 'заблокирован' if user_data['is_blocked'] else 'разблокирован'
                    logger.warning(f'Обнаружено изменение статуса блокировки для {username}: {status}')
                    if user_data['is_blocked']:
                        blocked_accounts.append(account)

                    account.name = user_data['name']
                    account.is_blocked = user_data['is_blocked']
                    storage.update_account(account.id, name=account.name, is_blocked=account.is_blocked)
                else:
                    continue

                changed_accounts.append({
                    'url': account.url,
                    'name': account.name or account.username,
                    'status': status,
                    'current_url': account.last_url
                })

            if url_changed_accounts or blocked_accounts:
                total_count = len(accounts)
                logger.info(f'Обнаружены изменения: {total_count} аккаунтов')

                unchanged_count = total_count - len(changed_accounts)
                url_changed_count = len(url_changed_accounts)
                blocked_count = len(blocked_accounts)

                blocked_header = '❌ Заблочены: {}'.format(blocked_count) if blocked_count else '✅ Заблочены: 0'
                url_header = '🔁 Смена URL: {}'.format(url_changed_count) if url_changed_count else '✅ Смена URL: 0'

                message_lines = [
                    '✅ Мониторинг Аккаунтов',
                    f'Всего: {total_count}',
                    f'Без изменений: {unchanged_count}\n',
                    blocked_header,
                    url_header
                ]

                if blocked_accounts:
                    message_lines.append('\nЗаблочены:')
                    for account in blocked_accounts:
                        message_lines.append(account.url)

                if url_changed_accounts:
                    message_lines.append('\nСмена URL:')
                    for item in url_changed_accounts:
                        message_lines.append(f'{item['user_url']} : {item['old_url']} > {item['new_url']}')

                inline_keyboard = InlineKeyboardBuilder()
                if blocked_accounts:
                    inline_keyboard.button(text='❌ Удалить невалид', callback_data=DeleteInvalidCallback())

                await bot.send_to_admins(
                    '\n'.join(message_lines),
                    reply_markup=inline_keyboard.as_markup()
                )

            if blocked_accounts:
                storage.add_last_failed_accounts(blocked_accounts)

            if changed_accounts:
                await sheets.update_monitor_accounts_data(changed_accounts)

        except Exception as e:
            logger.exception(f'Ошибка в мониторинге аккаунтов: {e}', exc_info=True)

        await asyncio.sleep(10)


async def schedule_monitor_posts_runner():
    while True:
        try:
            posts_settings = storage.get_monitor_posts_settings()
            if not should_monitor_posts_run(posts_settings):
                await asyncio.sleep(10)
                continue

            accounts = [
                account for account in storage.get_accounts()
                if posts_settings.accounts_mode == 'все' or posts_settings.accounts_mode == account.mode
            ]

            blocked_accounts = [account for account in accounts if account.is_blocked]
            active_accounts = [account for account in accounts if not account.is_blocked]

            logger.info(f'🔄 Запускаем мониторинг постов {len(active_accounts)} аккаунтов...')
            storage.update_monitor_posts_last_run()

            monitor_posts_ids = await sheets.get_monitor_posts_ids()
            accounts_by_id = {account.id: account for account in accounts}
            all_deleted_posts = []
            grouped_deleted_posts = {}

            for account in active_accounts:
                try:
                    logger.debug(f'Проверка постов для {account.username}')
                    if account.domain == 'tenchat.ru':
                        posts = await api.fetch_tenchat_posts(account.username)
                    else:
                        posts = await api.fetch_user_posts(account.domain, account.username)
                except Exception as e:
                    logger.error(f'Ошибка при получении постов для {account.username}: {e}', exc_info=True)
                    continue

                parsed_ids = {post['id'] for post in posts}
                existing_posts = await utils.load_user_posts(account.domain, account.username)

                deleted_posts = []
                for old_post in existing_posts:
                    if old_post['post_id'] not in parsed_ids and old_post['post_id'] not in monitor_posts_ids:
                        logger.info(f'Обнаружен удалённый пост: {old_post['post_id']} у {account.username}')
                        deleted_posts.append({
                            'account_url': account.url,
                            'name': account.name or account.username,
                            **old_post
                        })

                if deleted_posts:
                    all_deleted_posts.extend(deleted_posts)
                    grouped_deleted_posts[account.id] = deleted_posts

            if grouped_deleted_posts:
                total_deleted = len(all_deleted_posts)
                logger.warning(f'Обнаружено {total_deleted} удалённых постов')

                lines = [
                    '✅ Мониторинг Статей',
                    f'Проверенных аккаунтов: {len(active_accounts)}',
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
            else:
                logger.info('Удалённых постов не обнаружено')
                await bot.send_to_admins(
                    '\n✅ Мониторинг Статей'
                    f'\nПроверенных аккаунтов: {len(active_accounts)}'
                    f'\nЗаблоченных аккаунтов: {len(blocked_accounts)}'
                    '\n✅ Удаленных URL: 0'
                    '\n\nВсе ОК!'
                )

            await sheets.update_monitor_posts_data(all_deleted_posts)
        except Exception as error:
            logger.exception(f'Ошибка в мониторинге постов: {error}', exc_info=True)

        await asyncio.sleep(10)


@plugin.run()
async def start_schedules():
    asyncio.create_task(schedule_regular_parsing_runner())
    asyncio.create_task(schedule_monitor_accounts_runner())
    asyncio.create_task(schedule_monitor_posts_runner())
