import asyncio
import random
import time
from contextlib import suppress
from datetime import datetime

import gspread
import pytz
from gspread.exceptions import WorksheetNotFound
from gspread.utils import *
from gspread_formatting import *
from gspread_formatting import set_column_width, Color
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('google_credentials.json', scope)
client = gspread.authorize(creds)

MAIN_SHEET_NAME = 'Статистика по VC, DTF'
REGULAR_WORKSHEET_NAME = 'РЕГ.парс'
MOSCOW_TIMEZONE = pytz.timezone('Europe/Moscow')

COLUMN_WIDTHS = [
    57, 51, 68, 52, 55, 50, 53
]

DATE_FORMAT = {
    'numberFormat': {
        'type': 'DATE_TIME',
        'pattern': 'dd.MM.yy HH:mm:ss'
    }
}

NUMBER_FORMAT = {
    'numberFormat': {
        'type': 'NUMBER',
        'pattern': '#,##0'
    }
}


def sync_update_user_data(title: str, rows: list[dict]):
    spreadsheet = retry_with_backoff(client.open, MAIN_SHEET_NAME)
    worksheet = None

    with suppress(WorksheetNotFound):
        worksheet = spreadsheet.worksheet(title)
        worksheet.clear()

    if not worksheet:
        worksheet = spreadsheet.add_worksheet(title=title, rows=100, cols=20)
        worksheet.freeze(rows=1)

    headers = list(rows[0].keys()) + ['Дней с публикации', 'Просмотров/день', 'Ч и м', 'Мин']
    retry_with_backoff(worksheet.update, [headers], 'A1')

    values_to_insert = []
    for i, row in enumerate(rows, start=2):
        values = list(row.values())
        values.extend([
            f'=DATEDIF(E{i};G{i};"d")',
            f'=IF(H{i}=0;D{i}/1;ROUND(D{i}/H{i}))',
            f'=IF(AND(E{i}<>""; E{i + 1}<>""); INT(ABS(E{i + 1}-E{i})*24) & " ч " & ROUND(MOD(ABS(E{i + 1}-E{i})*24;1)*60;0) & " м"; "")',
            f'=IF(AND(E{i}<>""; E{i + 1}<>""); ROUND(ABS(E{i + 1}-E{i})*24*60; 0); "")'
        ])
        values_to_insert.append(values)

    retry_with_backoff(
        worksheet.update,
        values_to_insert,
        f'A2:Z{len(rows) + 1}',
        value_input_option=ValueInputOption.user_entered
    )

    batch_formats = [{
        'range': 'A1:Z1',
        'format': {
            'textFormat': {'bold': True}
        }
    }, {
        'range': f'E2:E{len(rows) + 1}',
        'format': {
            'numberFormat': {'type': 'DATE', 'pattern': 'd MMM'}
        }
    }, {
        'range': f'G2:G{len(rows) + 1}',
        'format': {
            'numberFormat': {'type': 'DATE', 'pattern': 'd MMM'}
        }
    }, {
        'range': f'D2:D{len(rows) + 1}',
        'format': {
            'numberFormat': {'type': 'NUMBER', 'pattern': '# ##0'}
        }
    }, {
        'range': f'H2:H{len(rows) + 1}',
        'format': {
            'numberFormat': {'type': 'NUMBER', 'pattern': '# ##0'}
        }
    }, {
        'range': f'I2:I{len(rows) + 1}',
        'format': {
            'numberFormat': {'type': 'NUMBER', 'pattern': '# ##0'}
        }
    }, {
        'range': 'A:A',
        'format': {
            'horizontalAlignment': 'LEFT'
        }
    }]

    retry_with_backoff(worksheet.batch_format, batch_formats)

    rules = retry_with_backoff(get_conditional_format_rules, worksheet)
    rules.clear()

    rule_days = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(f'H2:H{len(rows) + 1}', worksheet)],
        gradientRule=GradientRule(
            minpoint=InterpolationPoint(type='MIN', color=Color(0.345, 0.737, 0.549)),
            midpoint=InterpolationPoint(type='PERCENTILE', value='50', color=Color(1.0, 0.831, 0.392)),
            maxpoint=InterpolationPoint(type='MAX', color=Color(0.910, 0.486, 0.455))
        )
    )

    rule_views = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(f'I2:I{len(rows) + 1}', worksheet)],
        gradientRule=GradientRule(
            minpoint=InterpolationPoint(type='MIN', color=Color(0.910, 0.486, 0.455)),
            midpoint=InterpolationPoint(type='PERCENTILE', value='50', color=Color(1.0, 0.831, 0.392)),
            maxpoint=InterpolationPoint(type='MAX', color=Color(0.345, 0.737, 0.549))
        )
    )

    rule_min = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(f'K2:K{len(rows) + 1}', worksheet)],
        gradientRule=GradientRule(
            minpoint=InterpolationPoint(type='MIN', color=Color(0.345, 0.737, 0.549)),
            midpoint=InterpolationPoint(type='PERCENTILE', value='50', color=Color(1.0, 0.831, 0.392)),
            maxpoint=InterpolationPoint(type='MAX', color=Color(0.910, 0.486, 0.455))
        )
    )

    rules.append(rule_days)
    rules.append(rule_views)
    rules.append(rule_min)
    retry_with_backoff(rules.save)

    max_length = max(len(str(val)) for val in retry_with_backoff(worksheet.col_values, 1))
    retry_with_backoff(set_column_width, worksheet, 'A', max_length * 9)
    retry_with_backoff(set_column_width, worksheet, 'B', 100)
    retry_with_backoff(set_column_width, worksheet, 'C', 400)
    retry_with_backoff(set_column_width, worksheet, 'J', 71)
    retry_with_backoff(set_column_width, worksheet, 'K', 44)


async def update_user_data(title: str, rows: list[dict]):
    await asyncio.to_thread(sync_update_user_data, title, rows)


def sync_update_user_stats_table(users: list[dict]):
    spreadsheet = client.open(MAIN_SHEET_NAME)
    worksheet = spreadsheet.worksheet(REGULAR_WORKSHEET_NAME)

    worksheet.freeze(rows=2)
    existing_user_cells = retry_with_backoff(worksheet.row_values, 1)
    existing_users = {}

    for col in range(1, len(existing_user_cells) + 1, 8):
        url = existing_user_cells[col - 1]
        if url:
            existing_users[url] = col

    batch_updates = []
    batch_formats = []

    for user in users:
        url = user['url']
        name = user['name']
        today_posts = user['today_posts']
        today_views = user['today_views']
        total_posts = user['total_posts']
        total_views = user['total_views']

        if url in existing_users:
            user_col = existing_users[url]
        else:
            user_col = max(existing_users.values()) + 8 if existing_users else 1
            existing_users[url] = user_col

            col_letter = rowcol_to_a1(1, user_col).replace('1', '')
            retry_with_backoff(worksheet.add_cols, 7)
            batch_updates.append({
                'range': f'{col_letter}1',
                'values': [[url]]
            })

            batch_updates.append({
                'range': f'{col_letter}2',
                'values': [[
                    name, 'Постов', 'Просмотр', 'рзн-Пст', 'рзн-Прс', 'сег-Пст', 'сег-Прс'
                ]]
            })

            batch_formats.append({
                'range': f'{col_letter}2',
                'format': {
                    'textFormat': {
                        'bold': True
                    }
                }
            })

            for offset, width in enumerate(COLUMN_WIDTHS):
                col_l = rowcol_to_a1(1, user_col + offset).replace('1', '')
                retry_with_backoff(set_column_width, worksheet, col_l, width)

            spacer_col = user_col + 7
            spacer_letter = rowcol_to_a1(1, spacer_col).replace('1', '')
            retry_with_backoff(set_column_width, worksheet, spacer_letter, 10)

            batch_formats.append({
                'range': f'{spacer_letter}1:{spacer_letter}',
                'format': {
                    'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
                    'borders': {
                        'left': {
                            'style': 'SOLID',
                            'color': {'red': 0.1, 'green': 0.1, 'blue': 0.1}
                        },
                        'right': {
                            'style': 'SOLID',
                            'color': {'red': 0.1, 'green': 0.1, 'blue': 0.1}
                        }
                    }
                }
            })

        user_col_vals = retry_with_backoff(worksheet.col_values, user_col)
        data_rows = user_col_vals[2:]
        row_idx = len(data_rows) + 3
        prev_row = row_idx - 1
        today = datetime.now(MOSCOW_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')

        if prev_row < 3:
            diff_posts = '-'
            diff_views = '-'
        else:
            posts_col = rowcol_to_a1(1, user_col + 1).replace('1', '')
            views_col = rowcol_to_a1(1, user_col + 2).replace('1', '')
            diff_posts = f'={posts_col}{row_idx} - {posts_col}{prev_row}'
            diff_views = f'={views_col}{row_idx} - {views_col}{prev_row}'

        user_row = [
            today, total_posts, total_views,
            diff_posts, diff_views, today_posts, today_views
        ]

        batch_updates.append({
            'range': f'{rowcol_to_a1(row_idx, user_col)}',
            'values': [user_row]
        })

    retry_with_backoff(worksheet.batch_update, batch_updates, value_input_option='USER_ENTERED')
    last_row = worksheet.row_count

    for user_col in existing_users.values():
        date_col = rowcol_to_a1(1, user_col).replace('1', '')
        batch_formats.append({
            'range': f'{date_col}3:{date_col}{last_row}',
            'format': DATE_FORMAT
        })

        for offset in [1, 2, 3, 4, 5, 6]:
            col_l = rowcol_to_a1(1, user_col + offset).replace('1', '')
            batch_formats.append({
                'range': f'{col_l}3:{col_l}{last_row}',
                'format': NUMBER_FORMAT
            })

    retry_with_backoff(worksheet.batch_format, batch_formats)


async def update_user_stats_table(users: list[dict]):
    await asyncio.to_thread(sync_update_user_stats_table, users)


def retry_with_backoff(func, *args, attempt=5, **kwargs):
    for attempt in range(attempt):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait = 4 ** attempt + random.uniform(0, 1)
            print(f'⏳ Ошибка: {e}. Ждём {wait:.1f} сек...')
            time.sleep(wait)

    raise RuntimeError(f'❌ Превышено число попыток вызова {func.__name__}', e)
