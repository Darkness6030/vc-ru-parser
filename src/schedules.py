import asyncio
import logging
from datetime import datetime
from typing import Optional

import pytz
from rewire import simple_plugin

from src import storage, utils, api, sheets
from src.storage import Account, Periodicity

plugin = simple_plugin()

MOSCOW_TIMEZONE = pytz.timezone('Europe/Moscow')
SEMAPHORE = asyncio.Semaphore(5)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)


async def parse_account(account: Account, mode: Optional[str] = None):
    async with SEMAPHORE:
        domain, username, user_id = account.domain, account.username, account.user_id

        try:
            if domain == 'tenchat.ru':
                user_posts = await api.fetch_tenchat_posts(username, posts_amount=100, last_post_id=account.last_post_id)
            else:
                user_posts = await api.fetch_user_posts(domain, user_id, posts_amount=100, last_post_id=account.last_post_id)
        except Exception as e:
            logging.error(f'Ошибка при получении постов для {username}: {e}')
            raise

        logging.info(f'Получены {len(user_posts)} постов для {username}')
        if not user_posts:
            return

        try:
            user_data = utils.extract_tenchat_user_data(username, user_posts) \
                if domain == 'tenchat.ru' else \
                utils.extract_user_data(domain, username, user_posts)

            await sheets.update_user_stats_table([user_data])
            mode = mode or account.mode

            if mode in ('серв', 'оба'):
                await utils.download_posts_files(domain, username, user_posts)
                logging.info(f'Файлы {username} сохранены на сервер')

            if mode in ('табл', 'оба'):
                await utils.unload_posts_to_sheets(domain, username, user_posts)
                logging.info(f'Посты {username} выгружены в Google таблицу')

            last_post_id = user_posts[0]['id']
            storage.update_account(account.id, last_post_id=last_post_id)
        except Exception as e:
            logging.error(f'Ошибка при обработке данных для {username}: {e}')
            raise


def should_run_today(periodicity: Periodicity) -> bool:
    now = datetime.now(MOSCOW_TIMEZONE)

    if periodicity.last_run:
        last_run = periodicity.last_run.replace(tzinfo=MOSCOW_TIMEZONE)
        days_passed = (now.date() - last_run.date()).days
        if days_passed < periodicity.interval:
            return False

    target_time = datetime.combine(now.date(), periodicity.time)
    target_time = MOSCOW_TIMEZONE.localize(target_time)

    return now >= target_time


async def schedule_runner():
    while True:
        try:
            storage_data = storage.load_storage()
            periodicity = storage_data.periodicity

            if not periodicity:
                logging.warning('Нет настроек периодичности. Засыпаю...')
                await asyncio.sleep(1)
                continue

            if storage_data.paused or not should_run_today(periodicity):
                await asyncio.sleep(1)
                continue

            logging.info('🚀 Начат плановый парсинг аккаунтов...')
            storage.update_last_run()

            tasks = [asyncio.create_task(parse_account(account)) for account in storage_data.accounts]
            await asyncio.gather(*tasks)

            logging.info(f'✅ Парсинг завершён. Следующий запуск через {periodicity.interval} дней.')
        except Exception as e:
            logging.exception(f'Ошибка в планировщике: {e}')
            await asyncio.sleep(1)


@plugin.run()
async def start_schedules():
    asyncio.create_task(schedule_runner())
