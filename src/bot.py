from io import BytesIO
from typing import List

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BufferedInputFile
from aiogram.utils.callback_answer import CallbackAnswerMiddleware
from pydantic import BaseModel
from rewire import config, simple_plugin, DependenciesModule

plugin = simple_plugin()


@config
class Config(BaseModel):
    token: str
    admin_ids: List[int]


@plugin.setup()
async def create_bot() -> Bot:
    return Bot(Config.token, default=DefaultBotProperties(parse_mode=ParseMode.HTML, link_preview_is_disabled=True))


@plugin.setup()
async def create_dispatcher() -> Dispatcher:
    return Dispatcher(storage=MemoryStorage())


@plugin.setup()
async def add_middleware(dispatcher: Dispatcher):
    dispatcher.callback_query.middleware(CallbackAnswerMiddleware())


@plugin.run()
async def start_bot(bot: Bot, dispatcher: Dispatcher):
    await dispatcher.start_polling(bot, allowed_updates=dispatcher.resolve_used_update_types())


def is_admin(user_id: int) -> bool:
    return user_id in Config.admin_ids


async def send_to_admins(text: str, **kwargs):
    if len(text) >= 3000:
        text_lines = text.strip().splitlines()
        text_bytes = BytesIO(text.encode('utf-8'))

        caption_lines = text_lines[:2]
        caption_text = '\n'.join(caption_lines) + '\n...'

        for admin_id in Config.admin_ids:
            await get_bot().send_document(
                admin_id,
                BufferedInputFile(text_bytes.getvalue(), filename='message.txt'),
                caption=caption_text
            )
    else:
        for admin_id in Config.admin_ids:
            await get_bot().send_message(admin_id, text, **kwargs)


def get_bot() -> Bot:
    return DependenciesModule.get().resolve(Bot)
