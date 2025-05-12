import asyncio
from contextlib import suppress
from datetime import datetime

import gspread
from gspread.exceptions import WorksheetNotFound
from gspread.utils import ValueInputOption
from gspread_formatting import *
from gspread_formatting import set_column_width, Color
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('google_credentials.json', scope)
client = gspread.authorize(creds)

POSTS_SHEET_NAME = 'Статистика по VC, DTF'
REGULAR_SHEET_NAME = 'Регулярный парсинг'


def sync_update_user_data(title: str, rows: list[dict]):
    spreadsheet = client.open(POSTS_SHEET_NAME)
    worksheet = None

    with suppress(WorksheetNotFound):
        worksheet = spreadsheet.worksheet(title)
        worksheet.clear()

    if not worksheet:
        worksheet = spreadsheet.add_worksheet(title=title, rows=100, cols=20)

    worksheet.freeze(rows=1)
    headers = list(rows[0].keys())
    headers.extend(['Дней с публикации', 'Просмотров/день'])
    worksheet.update([headers], 'A1')

    values_to_insert = []
    for i, row in enumerate(rows, start=2):
        values = list(row.values())
        values.extend([
            f'=DATEDIF(E{i};G{i};"d")',
            f'=IF(H{i}=0;D{i}/1;ROUND(D{i}/H{i}))'
        ])
        values_to_insert.append(values)

    if values_to_insert:
        worksheet.update(values_to_insert, f'A2:Z{len(rows) + 1}', value_input_option=ValueInputOption.user_entered)

    headers_format = CellFormat(textFormat=TextFormat(bold=True))
    format_cell_range(worksheet, 'A1:Z1', headers_format)

    date_format = CellFormat(numberFormat=NumberFormat(type='DATE', pattern='d MMM'))
    format_cell_range(worksheet, f'E2:E{len(rows) + 1}', date_format)
    format_cell_range(worksheet, f'G2:G{len(rows) + 1}', date_format)

    number_format = CellFormat(numberFormat=NumberFormat(type='NUMBER', pattern='# ##0'))
    format_cell_range(worksheet, f'D2:D{len(rows) + 1}', number_format)
    format_cell_range(worksheet, f'H2:H{len(rows) + 1}', number_format)
    format_cell_range(worksheet, f'I2:I{len(rows) + 1}', number_format)

    rules = get_conditional_format_rules(worksheet)
    rules.clear()

    days_range = f'H2:H{len(rows) + 1}'
    views_range = f'I2:I{len(rows) + 1}'

    rule_days = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(days_range, worksheet)],
        gradientRule=GradientRule(
            minpoint=InterpolationPoint(type='MIN', color=Color(0.345, 0.737, 0.549)),
            midpoint=InterpolationPoint(type='PERCENTILE', value='50', color=Color(1.0, 0.831, 0.392)),
            maxpoint=InterpolationPoint(type='MAX', color=Color(0.910, 0.486, 0.455))
        )
    )

    rule_views = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(views_range, worksheet)],
        gradientRule=GradientRule(
            minpoint=InterpolationPoint(type='MIN', color=Color(0.910, 0.486, 0.455)),
            midpoint=InterpolationPoint(type='PERCENTILE', value='50', color=Color(1.0, 0.831, 0.392)),
            maxpoint=InterpolationPoint(type='MAX', color=Color(0.345, 0.737, 0.549))
        )
    )

    rules.append(rule_days)
    rules.append(rule_views)
    rules.save()

    alignment_format = CellFormat(horizontalAlignment='LEFT')
    format_cell_range(worksheet, 'A:A', alignment_format)

    max_length = max(len(str(value)) for value in worksheet.col_values(1))
    set_column_width(worksheet, 'A', max_length * 9)
    set_column_width(worksheet, 'B', 100)
    set_column_width(worksheet, 'C', 400)


async def update_user_data(title: str, rows: list[dict]):
    await asyncio.to_thread(sync_update_user_data, title, rows)


def sync_update_user_stats_table(users: list[dict]):
    print(users)

    spreadsheet = client.open(REGULAR_SHEET_NAME)
    worksheet = spreadsheet.get_worksheet(0)

    worksheet.freeze(rows=2)
    col_idx = 1

    existing_user_cells = worksheet.row_values(2)
    existing_users = {}

    while col_idx <= len(existing_user_cells):
        cell_value = existing_user_cells[col_idx - 1]
        if cell_value:
            existing_users[cell_value] = col_idx
        col_idx += 8

    for user in users:
        url = user['url']
        name = user['name']
        today_posts = user['today_posts']
        today_views = user['today_views']
        total_posts = user['total_posts']
        total_views = user['total_views']

        if name in existing_users:
            user_col = existing_users[name]
        else:
            user_col = max(existing_users.values()) + 8 if existing_users else 1
            existing_users[name] = user_col

            col_letter = gspread.utils.rowcol_to_a1(1, user_col).replace('1', '')
            worksheet.update([[url]], f"{col_letter}1")

            user_headers = [
                name,
                'Постов', 'Просмотр', 'рзн-Пст', 'рзн-Прс', 'сег-Пст', 'сег-Прс'
            ]

            worksheet.update([user_headers], f"{col_letter}2")
            format_cell_range(worksheet, f"{col_letter}2", CellFormat(textFormat=TextFormat(bold=True)))

            col_widths = [59, 51, 68, 52, 55, 50, 53]
            for offset, width in enumerate(col_widths):
                col_letter_w = gspread.utils.rowcol_to_a1(1, user_col + offset).replace('1', '')
                set_column_width(worksheet, col_letter_w, width)

            spacer_col = user_col + 7
            spacer_letter = gspread.utils.rowcol_to_a1(1, spacer_col).replace('1', '')
            set_column_width(worksheet, spacer_letter, 10)
            format_cell_range(worksheet, f"{spacer_letter}1:{spacer_letter}", CellFormat(
                backgroundColor=Color(0.9, 0.9, 0.9),
                borders=Borders(
                    left=Border(style='SOLID', color=Color(0.1, 0.1, 0.1)),
                    right=Border(style='SOLID', color=Color(0.1, 0.1, 0.1)),
                )
            ))

        posts_col_letter = gspread.utils.rowcol_to_a1(1, user_col).replace('1', '')
        existing_posts = worksheet.col_values(user_col)

        row_to_insert = len(existing_posts) + 1
        today = datetime.now().strftime('%Y-%m-%d')

        prev_row = row_to_insert - 1
        if prev_row < 3:
            diff_posts_formula = '-'
            diff_views_formula = '-'
        else:
            total_posts_col_letter = gspread.utils.rowcol_to_a1(1, user_col + 1).replace('1', '')
            total_views_col_letter = gspread.utils.rowcol_to_a1(1, user_col + 2).replace('1', '')
            diff_posts_formula = f"={total_posts_col_letter}{row_to_insert} - {total_posts_col_letter}{prev_row}"
            diff_views_formula = f"={total_views_col_letter}{row_to_insert} - {total_views_col_letter}{prev_row}"

        user_row = [
            today,
            total_posts,
            total_views,
            diff_posts_formula,
            diff_views_formula,
            today_posts,
            today_views
        ]

        range_to_update = f"{posts_col_letter}{row_to_insert}"
        worksheet.update([user_row], range_to_update, value_input_option=ValueInputOption.user_entered)

    date_format = CellFormat(numberFormat=NumberFormat(type='DATE', pattern='dd.MM.yy'))
    number_format = CellFormat(numberFormat=NumberFormat(type='NUMBER', pattern='# ##0'))
    last_row = worksheet.row_count

    for user_col in existing_users.values():
        date_col_letter = gspread.utils.rowcol_to_a1(1, user_col).replace('1', '')
        format_cell_range(worksheet, f"{date_col_letter}3:{date_col_letter}{last_row}", date_format)

        col_letter1 = gspread.utils.rowcol_to_a1(1, user_col + 1).replace('1', '')
        col_letter2 = gspread.utils.rowcol_to_a1(1, user_col + 2).replace('1', '')
        format_cell_range(worksheet, f"{col_letter1}3:{col_letter1}{last_row}", number_format)
        format_cell_range(worksheet, f"{col_letter2}3:{col_letter2}{last_row}", number_format)


async def update_user_stats_table(users: list[dict]):
    await asyncio.to_thread(sync_update_user_stats_table, users)
