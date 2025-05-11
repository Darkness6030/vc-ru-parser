import asyncio
from contextlib import suppress

import gspread
from gspread.exceptions import WorksheetNotFound
from gspread.utils import ValueInputOption
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("google_credentials.json", scope)
client = gspread.authorize(creds)

SHEET_NAME = 'Статистика по VC, DTF'


def sync_update_user_data(title: str, rows: list):
    spreadsheet = client.open(SHEET_NAME)
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


async def update_user_data(title: str, rows: list):
    await asyncio.to_thread(sync_update_user_data, title, rows)
