from aiogram.filters.callback_data import CallbackData


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
    mode: str


class PeriodicityCallback(CallbackData, prefix='periodicity'):
    pass


class AccountsCallback(CallbackData, prefix='accounts'):
    pass


class TogglePauseCallback(CallbackData, prefix='start_pause'):
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
