from typing import Union

from aiogram.filters.callback_data import CallbackData
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


class InlineKeyboard(InlineKeyboardMarkup):
    def __init__(self):
        super().__init__(inline_keyboard=[])

    def add_button(
            self,
            button: Union[str, InlineKeyboardButton],
            callback_data: Union[str, CallbackData, None] = None,
            row_width=1,
            condition: bool = True,
            **kwargs
    ):
        if not condition:
            return

        if not isinstance(button, InlineKeyboardButton):
            callback_data = callback_data.pack() if isinstance(callback_data, CallbackData) else callback_data
            button = InlineKeyboardButton(text=button, callback_data=callback_data, **kwargs)

        if not self.inline_keyboard or len(self.inline_keyboard[-1]) >= row_width:
            self.inline_keyboard.append([])

        self.inline_keyboard[-1].append(button)
        return self
