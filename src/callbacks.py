from aiogram.filters.callback_data import CallbackData


class LoadModeCallback(CallbackData, prefix='load_mode'):
    mode: str


class ParseAmountCallback(CallbackData, prefix='parse_amount'):
    pass


class ParseAllCallback(CallbackData, prefix='parse_all'):
    pass


class CancelParsingCallback(CallbackData, prefix='cancel_parsing'):
    pass
