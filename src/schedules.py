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


async def parse_account_posts(account: Account, mode: Optional[str] = None):
    async with SEMAPHORE:
        domain, username, user_id = account.domain, account.username, account.user_id
        user_data = await api.fetch_tenchat_user_data(username) \
            if domain == 'tenchat.ru' else \
            await api.fetch_user_data(domain, id=user_id)

        account.name = user_data['name']
        storage.update_account(account.id, url=account.url, name=account.name)

        if user_data['is_blocked']:
            logger.error(f'Аккаунт {username} заблокирован')
            raise

        try:
            logger.info(f'Получаем посты для {username}...')
            if domain == 'tenchat.ru':
                user_posts = await api.fetch_tenchat_posts(username)
            else:
                user_posts = await api.fetch_user_posts(domain, user_id)
        except Exception as e:
            logger.error(f'Ошибка при получении постов для {username}: {e}', exc_info=True)
            raise

        logger.info(f'Получены {len(user_posts)} постов для {username}')
        if not user_posts:
            return

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


def should_regular_parsing_run(settings: RegularParsingSettings) -> bool:
    if not settings.enabled or not settings.periodicity:
        return False

    now = datetime.now(MOSCOW_TIMEZONE)
    if settings.last_run:
        last_run = settings.last_run.replace(tzinfo=MOSCOW_TIMEZONE)
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
        last_run = settings.last_run.replace(tzinfo=MOSCOW_TIMEZONE)
        minutes_passed = (now - last_run).total_seconds() / 60
        if minutes_passed < settings.periodicity:
            return False

    return True


def should_monitor_posts_run(settings: MonitorPostsSettings) -> bool:
    if not settings.enabled or not settings.periodicity:
        return False

    now = datetime.now(MOSCOW_TIMEZONE)
    today = now.date()

    if settings.last_run and settings.last_run.date() == today:
        latest_time = max(settings.periodicity)
        if now.time() < latest_time:
            return False

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

            logger.info(f'🚀 Начат плановый парсинг {len(storage.get_accounts())} аккаунтов...')
            storage.update_regular_parsing_last_run()

            success_count = 0
            failed_count = 0
            failed_accounts = []

            async def safe_parse(account):
                nonlocal success_count, failed_count
                try:
                    await parse_account_posts(account)
                    success_count += 1
                except Exception:
                    failed_count += 1
                    failed_accounts.append(account)

            tasks = [safe_parse(account) for account in storage.get_accounts() if not account.is_blocked]
            await asyncio.gather(*tasks)

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

                for account in failed_accounts:
                    account.is_blocked = True
                    storage.update_account(account.id, is_blocked=True)

                inline_keyboard.button(text='❌ Удалить невалид', callback_data=DeleteInvalidCallback())
                storage.set_last_failed_accounts(failed_accounts)

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
                domain, username, user_id = account.domain, account.username, account.user_id
                logger.debug(f'Проверка аккаунта {username} (ID: {user_id})')

                try:
                    user_data = await api.fetch_tenchat_user_data(username) \
                        if domain == 'tenchat.ru' else \
                        await api.fetch_user_data(domain, id=user_id)
                except Exception as e:
                    logger.error(f'Ошибка при получении данных для {account.username}: {e}', exc_info=True)
                    user_data = {'url': account.last_url, 'name': account.name, 'is_blocked': True}

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
                        blocked_accounts.append(account.username)

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
                    for username in blocked_accounts:
                        message_lines.append(f'{username}')

                if url_changed_accounts:
                    message_lines.append('\nСмена URL:')
                    for item in url_changed_accounts:
                        message_lines.append(f'{item['user_url']} : {item['old_url']} > {item['new_url']}')

                await bot.send_to_admins('\n'.join(message_lines))

            if changed_accounts:
                await sheets.update_monitor_accounts_data(changed_accounts)
        except Exception as e:
            logger.exception(f'Ошибка в мониторинге аккаунтов: {e}', exc_info=True)

        await asyncio.sleep(10)


async def schedule_monitor_posts_runner():
    while True:
        try:
            monitor_posts_settings = storage.get_monitor_posts_settings()
            if not should_monitor_posts_run(monitor_posts_settings):
                await asyncio.sleep(10)
                continue

            logger.info('🔄 Запуск мониторинга постов...')
            accounts = [account for account in storage.get_accounts() if account.mode == 'оба']
            storage.update_monitor_posts_last_run()

            all_deleted_posts = []
            grouped_deleted_posts = {}
            accounts_by_id = {}

            for account in accounts:
                if account.is_blocked:
                    continue

                logger.debug(f'Проверка постов для {account.username}')

                try:
                    if account.domain == 'tenchat.ru':
                        posts = await api.fetch_tenchat_posts(account.username)
                    else:
                        posts = await api.fetch_user_posts(account.domain, account.user_id)
                except Exception as e:
                    logger.error(f'Ошибка при получении постов для {account.username}: {e}', exc_info=True)
                    continue

                parsed_ids = {post['id'] for post in posts}
                existing_posts = await utils.load_user_posts(account.domain, account.username)

                deleted_posts = []
                for old_post in existing_posts:
                    if old_post['post_id'] not in parsed_ids:
                        logger.info(f'Обнаружен удалённый пост: {old_post['post_id']} у {account.username}')
                        deleted_posts.append({
                            'account_url': account.url,
                            'name': account.name or account.username,
                            **old_post
                        })

                if deleted_posts:
                    all_deleted_posts.extend(deleted_posts)
                    grouped_deleted_posts[account.id] = deleted_posts
                    accounts_by_id[account.id] = account

            if grouped_deleted_posts:
                total_deleted = len(all_deleted_posts)
                logger.warning(f'Обнаружено {len(all_deleted_posts)} удалённых постов')

                total_accounts = len(accounts)
                lines = [
                    '✅ Мониторинг Статей',
                    f'Проверенных аккаунтов: {total_accounts}',
                    f'❌ Удаленных URL: {total_deleted}'
                ]

                for account_id, posts in grouped_deleted_posts.items():
                    account = accounts_by_id[account_id]
                    lines.append(f'\n{account.username} - {len(posts)}:')
                    for post in posts:
                        lines.append(f'{post['post_id']}')
                    for post in posts:
                        lines.append(f'{post['post_url']}')

                await bot.send_to_admins('\n'.join(lines))
            else:
                logger.info('Удалённых постов не обнаружено')
                await bot.send_to_admins(
                    '\n✅ Мониторинг Статей'
                    f'\nПроверенных аккаунтов: {sum(1 for account in accounts if not account.is_blocked)}'
                    f'\nЗаблоченных аккаунтов: {sum(1 for account in accounts if account.is_blocked)}'
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
