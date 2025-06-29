import asyncio
import random
import time
from contextlib import suppress
from datetime import datetime, timedelta

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

MAIN_SHEET = 'Статистика по VC, DTF'
REGULAR_PARSING_WORKSHEET = 'РЕГ.парс'
MONITOR_ACCOUNTS_WORKSHEET = 'Монит.Акков'
MONITOR_POSTS_WORKSHEET = 'Монит.Статей'

MOSCOW_TIMEZONE = pytz.timezone('Europe/Moscow')
GOOGLE_SHEETS_EPOCH = datetime(1899, 12, 30)

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


def from_serial_date(serial: float) -> datetime:
    return GOOGLE_SHEETS_EPOCH + timedelta(days=serial)


def sync_get_user_data(title: str) -> list[dict]:
    try:
        spreadsheet = run_with_retry(client.open, MAIN_SHEET)
        worksheet = spreadsheet.worksheet(title)
    except WorksheetNotFound:
        return []

    all_data = run_with_retry(worksheet.get_all_values, value_render_option='UNFORMATTED_VALUE')
    if not all_data or len(all_data) < 2:
        return []

    headers = all_data[0]
    data_rows = all_data[1:]

    id_index = headers.index('ID')
    url_index = headers.index('URL')
    title_index = headers.index('Название статьи')
    views_index = headers.index('Просмотры')
    date_index = headers.index('Добавлено')

    parsed_data = []
    for row in data_rows:
        if len(row) < max(id_index, url_index, title_index, views_index, date_index) + 1:
            continue

        parsed_data.append({
            'post_id': int(row[id_index]),
            'post_url': row[url_index],
            'post_title': row[title_index],
            'views': int(row[views_index]),
            'publish_date': from_serial_date(row[date_index])
        })

    return parsed_data


async def get_user_data(title: str) -> list[dict]:
    return await asyncio.to_thread(sync_get_user_data, title)


def sync_update_user_data(title: str, users_data: list[dict]):
    spreadsheet = run_with_retry(client.open, MAIN_SHEET)
    worksheet = None

    with suppress(WorksheetNotFound):
        worksheet = spreadsheet.worksheet(title)
        worksheet.clear()

    if not worksheet:
        worksheet = spreadsheet.add_worksheet(title=title, rows=100, cols=20)
        worksheet.freeze(rows=1)

    headers = list(users_data[0].keys()) + ['Дней с публикации', 'Просмотров/день', 'Ч и м', 'Мин']
    run_with_retry(worksheet.update, [headers], 'A1')

    values_to_insert = []
    for index, row in enumerate(users_data, start=2):
        values = list(row.values())
        values.extend([
            f'=DATEDIF(E{index};G{index};"d")',
            f'=IF(H{index}=0;D{index}/1;ROUND(D{index}/H{index}))',
            f'=IF(AND(E{index}<>""; E{index + 1}<>""); INT(ABS(E{index + 1}-E{index})*24) & " ч " & ROUND(MOD(ABS(E{index + 1}-E{index})*24;1)*60;0) & " м"; "")',
            f'=IF(AND(E{index}<>""; E{index + 1}<>""); ROUND(ABS(E{index + 1}-E{index})*24*60; 0); "")'
        ])
        values_to_insert.append(values)

    run_with_retry(
        worksheet.update,
        values_to_insert,
        f'A2:Z{len(users_data) + 1}',
        value_input_option=ValueInputOption.user_entered
    )

    batch_formats = [{
        'range': 'A1:Z1',
        'format': {
            'textFormat': {'bold': True}
        }
    }, {
        'range': f'E2:E{len(users_data) + 1}',
        'format': {
            'numberFormat': {'type': 'DATE', 'pattern': 'd MMM'}
        }
    }, {
        'range': f'G2:G{len(users_data) + 1}',
        'format': {
            'numberFormat': {'type': 'DATE', 'pattern': 'd MMM'}
        }
    }, {
        'range': f'D2:D{len(users_data) + 1}',
        'format': {
            'numberFormat': {'type': 'NUMBER', 'pattern': '# ##0'}
        }
    }, {
        'range': f'H2:H{len(users_data) + 1}',
        'format': {
            'numberFormat': {'type': 'NUMBER', 'pattern': '# ##0'}
        }
    }, {
        'range': f'I2:I{len(users_data) + 1}',
        'format': {
            'numberFormat': {'type': 'NUMBER', 'pattern': '# ##0'}
        }
    }, {
        'range': 'A:A',
        'format': {
            'horizontalAlignment': 'LEFT'
        }
    }]

    rules = run_with_retry(get_conditional_format_rules, worksheet)
    rules.clear()

    rules.append(ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(f'H2:H{len(users_data) + 1}', worksheet)],
        gradientRule=GradientRule(
            minpoint=InterpolationPoint(type='MIN', color=Color(0.345, 0.737, 0.549)),
            midpoint=InterpolationPoint(type='PERCENTILE', value='50', color=Color(1.0, 0.831, 0.392)),
            maxpoint=InterpolationPoint(type='MAX', color=Color(0.910, 0.486, 0.455))
        )
    ))

    rules.append(ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(f'I2:I{len(users_data) + 1}', worksheet)],
        gradientRule=GradientRule(
            minpoint=InterpolationPoint(type='MIN', color=Color(0.910, 0.486, 0.455)),
            midpoint=InterpolationPoint(type='PERCENTILE', value='50', color=Color(1.0, 0.831, 0.392)),
            maxpoint=InterpolationPoint(type='MAX', color=Color(0.345, 0.737, 0.549))
        )
    ))

    rules.append(ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(f'K2:K{len(users_data) + 1}', worksheet)],
        gradientRule=GradientRule(
            minpoint=InterpolationPoint(type='MIN', color=Color(0.345, 0.737, 0.549)),
            midpoint=InterpolationPoint(type='PERCENTILE', value='50', color=Color(1.0, 0.831, 0.392)),
            maxpoint=InterpolationPoint(type='MAX', color=Color(0.910, 0.486, 0.455))
        )
    ))

    run_with_retry(rules.save)
    run_with_retry(worksheet.batch_format, batch_formats)

    max_length = max(len(str(val)) for val in run_with_retry(worksheet.col_values, 1))
    run_with_retry(set_column_width, worksheet, 'A', max_length * 9)
    run_with_retry(set_column_width, worksheet, 'B', 100)
    run_with_retry(set_column_width, worksheet, 'C', 400)
    run_with_retry(set_column_width, worksheet, 'J', 71)
    run_with_retry(set_column_width, worksheet, 'K', 44)


async def update_user_data(title: str, users_data: list[dict]):
    await asyncio.to_thread(sync_update_user_data, title, users_data)


def sync_update_regular_parsing_data(users_data: list[dict]):
    spreadsheet = client.open(MAIN_SHEET)
    worksheet = spreadsheet.worksheet(REGULAR_PARSING_WORKSHEET)

    worksheet.freeze(rows=2)
    existing_user_cells = run_with_retry(worksheet.row_values, 1)
    existing_users = {}

    for col in range(1, len(existing_user_cells) + 1, 8):
        url = existing_user_cells[col - 1]
        if url:
            existing_users[url] = col

    batch_updates = []
    batch_formats = []

    for user in users_data:
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
            run_with_retry(worksheet.add_cols, 7)
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

            widths = [57, 51, 68, 52, 55, 50, 53]
            for offset, width in enumerate(widths):
                col_l = rowcol_to_a1(1, user_col + offset).replace('1', '')
                run_with_retry(set_column_width, worksheet, col_l, width)

            spacer_col = user_col + 7
            spacer_letter = rowcol_to_a1(1, spacer_col).replace('1', '')
            run_with_retry(set_column_width, worksheet, spacer_letter, 10)

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

        user_col_vals = run_with_retry(worksheet.col_values, user_col)
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

    run_with_retry(worksheet.batch_update, batch_updates, value_input_option='USER_ENTERED')
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

    run_with_retry(worksheet.batch_format, batch_formats)


async def update_regular_parsing_data(users_data: list[dict]):
    await asyncio.to_thread(sync_update_regular_parsing_data, users_data)


def sync_update_monitor_accounts_data(users_data: list[dict]):
    spreadsheet = client.open(MAIN_SHEET)
    worksheet = spreadsheet.worksheet(MONITOR_ACCOUNTS_WORKSHEET)

    worksheet.freeze(rows=2)
    existing_user_cells = run_with_retry(worksheet.row_values, 1)
    existing_users = {}

    for col in range(1, len(existing_user_cells) + 1, 5):
        url = existing_user_cells[col - 1]
        if url:
            existing_users[url] = col

    batch_updates = []
    batch_formats = []

    for user in users_data:
        url = user['url']
        name = user['name']
        status = user['status']
        current_url = user['current_url']

        if url in existing_users:
            user_col = existing_users[url]
        else:
            user_col = max(existing_users.values()) + 5 if existing_users else 1
            existing_users[url] = user_col

            col_letter = rowcol_to_a1(1, user_col).replace('1', '')
            run_with_retry(worksheet.add_cols, 3)
            batch_updates.append({
                'range': f'{col_letter}1',
                'values': [[url]]
            })

            batch_updates.append({
                'range': f'{col_letter}2',
                'values': [[
                    name, 'Статус', 'Нов.URL', 'рзн-ч'
                ]]
            })

            batch_formats.append({
                'range': f'{col_letter}2',
                'format': {
                    'textFormat': {'bold': True}
                }
            })

            widths = [91, 77, 255, 39]
            for offset, width in enumerate(widths):
                col_l = rowcol_to_a1(1, user_col + offset).replace('1', '')
                run_with_retry(set_column_width, worksheet, col_l, width)

            spacer_col = user_col + 4
            spacer_letter = rowcol_to_a1(1, spacer_col).replace('1', '')
            run_with_retry(set_column_width, worksheet, spacer_letter, 10)

            batch_formats.append({
                'range': f'{spacer_letter}1:{spacer_letter}',
                'format': {
                    'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
                    'borders': {
                        'left': {'style': 'SOLID', 'color': {'red': 0.1, 'green': 0.1, 'blue': 0.1}},
                        'right': {'style': 'SOLID', 'color': {'red': 0.1, 'green': 0.1, 'blue': 0.1}},
                    }
                }
            })

        user_col_vals = run_with_retry(worksheet.col_values, user_col)
        data_rows = user_col_vals[2:]
        row_idx = len(data_rows) + 3
        prev_row = row_idx - 1
        now = datetime.now(MOSCOW_TIMEZONE)

        date_col_letter = rowcol_to_a1(1, user_col).replace('1', '')
        if prev_row < 3:
            diff_formula = '-'
        else:
            diff_formula = f'=({date_col_letter}{row_idx} - {date_col_letter}{prev_row}) * 24'

        user_row = [
            now.strftime('%d.%m.%y %H:%M:%S'),
            status,
            current_url,
            diff_formula
        ]

        batch_updates.append({
            'range': f'{rowcol_to_a1(row_idx, user_col)}',
            'values': [user_row]
        })

    run_with_retry(worksheet.batch_update, batch_updates, value_input_option='USER_ENTERED')
    last_row = worksheet.row_count

    rules = run_with_retry(get_conditional_format_rules, worksheet)
    rules.clear()

    for user_col in existing_users.values():
        date_col = rowcol_to_a1(1, user_col).replace('1', '')
        batch_formats.append({
            'range': f'{date_col}3:{date_col}{last_row}',
            'format': DATE_FORMAT
        })

        diff_col = rowcol_to_a1(1, user_col + 3).replace('1', '')
        batch_formats.append({
            'range': f'{diff_col}3:{diff_col}{last_row}',
            'format': {
                'numberFormat': {
                    'type': 'NUMBER',
                    'pattern': '0'
                }
            }
        })

        rules.append(ConditionalFormatRule(
            ranges=[GridRange.from_a1_range(f'{diff_col}3:{diff_col}{last_row}', worksheet)],
            gradientRule=GradientRule(
                minpoint=InterpolationPoint(type='MIN', color=Color(0.345, 0.737, 0.549)),
                midpoint=InterpolationPoint(type='PERCENTILE', value='50', color=Color(1.0, 0.831, 0.392)),
                maxpoint=InterpolationPoint(type='MAX', color=Color(0.910, 0.486, 0.455))
            )
        ))

    run_with_retry(rules.save)
    run_with_retry(worksheet.batch_format, batch_formats)


async def update_monitor_accounts_data(users_data: list[dict]):
    await asyncio.to_thread(sync_update_monitor_accounts_data, users_data)


def sync_update_monitor_posts_data(posts_data: list[dict]):
    spreadsheet = client.open(MAIN_SHEET)
    worksheet = spreadsheet.worksheet(MONITOR_POSTS_WORKSHEET)

    header = [
        'Дата обнаруж', 'Аккаунт', 'ФИО', 'ID', 'URL',
        'Название статьи', 'Просмотры', 'Добавлено', 'Дн публ'
    ]

    existing_header = run_with_retry(worksheet.row_values, 1)
    if existing_header != header:
        run_with_retry(worksheet.update, 'A1:I1', [header])
        run_with_retry(worksheet.freeze, rows=1)

        widths = [91, 215, 169, 63, 100, 400, 75, 73, 55]
        for offset, width in enumerate(widths):
            col_letter = rowcol_to_a1(1, offset + 1).replace('1', '')
            run_with_retry(set_column_width, worksheet, col_letter, width)

    rows_to_append = []
    now = datetime.now(MOSCOW_TIMEZONE)

    data_rows = run_with_retry(worksheet.get_all_values)
    last_filled_row = len(data_rows)

    for index, post in enumerate(posts_data):
        row_number = last_filled_row + index + 1
        days_since_formula = f'=A{row_number} - H{row_number}'

        rows_to_append.append([
            now.strftime('%d.%m.%y %H:%M:%S'),
            post['account_url'],
            post['name'],
            post['post_id'],
            post['post_url'],
            post['post_title'],
            post['views'],
            post['publish_date'].strftime('%d.%m.%y %H:%M:%S'),
            days_since_formula
        ])

    if rows_to_append:
        run_with_retry(worksheet.append_rows, rows_to_append, value_input_option='USER_ENTERED')

    batch_formats = []
    last_row = worksheet.row_count

    rules = run_with_retry(get_conditional_format_rules, worksheet)
    rules.clear()

    for col_idx in [1, 8]:
        col_letter = rowcol_to_a1(1, col_idx).replace('1', '')
        batch_formats.append({
            'range': f'{col_letter}2:{col_letter}{last_row}',
            'format': DATE_FORMAT
        })

    col_letter = rowcol_to_a1(1, 9).replace('1', '')
    batch_formats.append({
        'range': f'{col_letter}2:{col_letter}{last_row}',
        'format': {
            'numberFormat': {
                'type': 'NUMBER',
                'pattern': '0'
            }
        }
    })

    views_col = rowcol_to_a1(1, 7).replace('1', '')
    rules.append(ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(f'{views_col}2:{views_col}{last_row}', worksheet)],
        gradientRule=GradientRule(
            minpoint=InterpolationPoint(type='MIN', color=Color(0.910, 0.486, 0.455)),
            midpoint=InterpolationPoint(type='PERCENTILE', value='50', color=Color(1.0, 0.831, 0.392)),
            maxpoint=InterpolationPoint(type='MAX', color=Color(0.345, 0.737, 0.549))
        )
    ))

    run_with_retry(rules.save)
    run_with_retry(worksheet.batch_format, batch_formats)


async def update_monitor_posts_data(posts_data: list[dict]):
    await asyncio.to_thread(sync_update_monitor_posts_data, posts_data)


def run_with_retry(func, *args, attempt=5, **kwargs):
    for attempt in range(attempt):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait = 4 ** attempt + random.uniform(0, 1)
            print(f'⏳ Ошибка: {e}. Ждём {wait:.1f} сек...')
            time.sleep(wait)

    raise RuntimeError(f'❌ Превышено число попыток вызова {func.__name__}', e)

