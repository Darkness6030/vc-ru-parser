from aiogram.fsm.state import State, StatesGroup


class UserState(StatesGroup):
    amount_select = State()
    amount_input = State()
    url_select = State()
    periodicity_input = State()
    add_account_input = State()
    edit_account_input = State()
