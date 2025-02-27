from aiogram.fsm.state import State, StatesGroup


class UserState(StatesGroup):
    amount = State()
    amount_input = State()
    url = State()
