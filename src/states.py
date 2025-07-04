from aiogram.fsm.state import State, StatesGroup


class UserState(StatesGroup):
    amount_select = State()
    amount_input = State()
    url_select = State()
    add_account = State()
    edit_account = State()
    username_links = State()
    regular_parsing_periodicity = State()
    monitor_accounts_periodicity = State()
    monitor_posts_periodicity = State()

