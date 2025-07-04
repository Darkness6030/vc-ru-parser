from typing import Optional

from aiogram.filters.callback_data import CallbackData
from aiogram.utils.keyboard import InlineKeyboardBuilder


class LoadModeCallback(CallbackData, prefix='load_mode'):
    mode: str


class ParseAmountCallback(CallbackData, prefix='parse_amount'):
    pass


class ParseAllCallback(CallbackData, prefix='parse_all'):
    pass


class CancelParsingCallback(CallbackData, prefix='cancel_parsing'):
    pass


class MainMenuCallback(CallbackData, prefix='main_menu'):
    pass


class RegularParsingCallback(CallbackData, prefix='regular_parsing'):
    pass


class ParseNowCallback(CallbackData, prefix='parse_now'):
    mode: Optional[str]
    blocked_mode: Optional[str] = None


class AccountsCallback(CallbackData, prefix='accounts'):
    pass


class RegularParsingToggleCallback(CallbackData, prefix='regular_parsing_toggle'):
    pass


class RegularParsingPeriodicityCallback(CallbackData, prefix='regular_parsing_periodicity'):
    pass


class AddAccountCallback(CallbackData, prefix='add_account'):
    pass


class AccountInfoCallback(CallbackData, prefix='account_info'):
    account_id: int


class EditAccountCallback(CallbackData, prefix='edit_account'):
    account_id: int


class DeleteAccountCallback(CallbackData, prefix='delete_account'):
    account_id: int


class ParseAccountCallback(CallbackData, prefix='parse_account'):
    account_id: int
    skip_validation: bool = False


class DeleteInvalidCallback(CallbackData, prefix='delete_invalid'):
    pass


class DeleteInvalidConfirmCallback(CallbackData, prefix='delete_invalid_confirm'):
    confirm: bool


class MonitorAccountsCallback(CallbackData, prefix='monitor_accounts'):
    pass


class MonitorAccountsToggleCallback(CallbackData, prefix='monitor_accounts_toggle'):
    pass


class MonitorAccountsPeriodicityCallback(CallbackData, prefix='monitor_accounts_periodicity'):
    pass


class MonitorAccountsToggleChangeURLCallback(CallbackData, prefix='monitor_accounts_toggle_change_url'):
    pass


class MonitorAccountsToggleBlockingCallback(CallbackData, prefix='monitor_accounts_toggle_blocking'):
    pass


class MonitorAccountsSitesCallback(CallbackData, prefix='monitor_accounts_sites'):
    toggle_dtf: bool = False
    toggle_vc: bool = False
    toggle_tenchat: bool = False


class MonitorPostsCallback(CallbackData, prefix='monitor_posts'):
    pass


class MonitorPostsToggleCallback(CallbackData, prefix='monitor_posts_toggle'):
    pass


class MonitorPostsPeriodicityCallback(CallbackData, prefix='monitor_posts_periodicity'):
    pass


class MonitorPostsAccountsModeCallback(CallbackData, prefix='monitor_posts_accounts_mode'):
    accounts_mode: Optional[str] = None


class MonitorPostsSitesCallback(CallbackData, prefix='monitor_posts_sites'):
    toggle_dtf: bool = False
    toggle_vc: bool = False
    toggle_tenchat: bool = False


class ParseBlockedConfirmCallback(CallbackData, prefix='parse_confirm'):
    mode: str
    username: str


class ParseBlockedCancelCallback(CallbackData, prefix='parse_cancel'):
    pass


class ParseIDsCallback(CallbackData, prefix='parse_ids'):
    pass


menu_keyboard = InlineKeyboardBuilder() \
    .button(text='Выгрузить данные в JSON', callback_data=LoadModeCallback(mode='server')) \
    .button(text='Выгрузить данные в Google таблицы', callback_data=LoadModeCallback(mode='sheets')) \
    .button(text='Парсинг по расписанию', callback_data=RegularParsingCallback()) \
    .button(text='Получить ID', callback_data=ParseIDsCallback()) \
    .adjust(1) \
    .as_markup()

regular_parsing_keyboard = InlineKeyboardBuilder() \
    .button(text='Назад', callback_data=RegularParsingCallback()) \
    .button(text='Назад в меню', callback_data=MainMenuCallback()) \
    .as_markup()

monitor_accounts_keyboard = InlineKeyboardBuilder() \
    .button(text='Назад', callback_data=MonitorAccountsCallback()) \
    .button(text='Назад в меню', callback_data=MainMenuCallback()) \
    .as_markup()

monitor_posts_keyboard = InlineKeyboardBuilder() \
    .button(text='Назад', callback_data=MonitorPostsCallback()) \
    .button(text='Назад в меню', callback_data=MainMenuCallback()) \
    .as_markup()
