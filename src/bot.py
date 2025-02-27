from typing import List

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
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


def get_bot() -> Bot:
    return DependenciesModule.get().resolve(Bot)
