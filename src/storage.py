import os.path
from datetime import datetime, time
from typing import List, Optional

from pydantic import BaseModel

STORAGE_PATH = 'storage/storage.json'


class Account(BaseModel):
    id: int
    url: str
    mode: str
    domain: str
    username: str
    name: Optional[str] = None
    user_id: Optional[int] = None
    last_post_id: Optional[int] = None


class Periodicity(BaseModel):
    interval: int
    time: time
    last_run: Optional[datetime] = None


class StorageData(BaseModel):
    accounts: List[Account] = []
    last_failed_accounts: List[Account] = []
    periodicity: Optional[Periodicity] = None
    paused: bool = False


def load_storage() -> StorageData:
    if not os.path.exists(STORAGE_PATH):
        return StorageData()

    with open(STORAGE_PATH, 'r', encoding='utf-8') as file:
        return StorageData.model_validate_json(file.read())


def save_storage(data: StorageData):
    with open(STORAGE_PATH, 'w', encoding='utf-8') as file:
        file.write(data.model_dump_json(indent=2))


def get_accounts() -> List[Account]:
    return load_storage().accounts


def get_account(account_id: int) -> Optional[Account]:
    for account in get_accounts():
        if account.id == account_id:
            return account


def add_account(**kwargs):
    data = load_storage()
    data.accounts.append(Account(id=get_next_account_id(data.accounts), **kwargs))
    save_storage(data)


def update_account(account_id: int, **kwargs):
    data = load_storage()
    for account in data.accounts:
        if account.id == account_id:
            account.__dict__.update(**kwargs)
            save_storage(data)


def delete_account(account_id: int):
    data = load_storage()
    data.accounts = [account for account in data.accounts if account.id != account_id]
    save_storage(data)


def get_last_failed_accounts() -> List[Account]:
    return load_storage().last_failed_accounts


def set_last_failed_accounts(last_failed_accounts: List[Account]):
    data = load_storage()
    data.last_failed_accounts = last_failed_accounts
    save_storage(data)


def get_periodicity() -> Optional[Periodicity]:
    return load_storage().periodicity


def set_periodicity(interval: int, time: str):
    data = load_storage()
    data.periodicity = Periodicity(interval=interval, time=time)
    save_storage(data)


def update_last_run():
    data = load_storage()
    if data.periodicity:
        data.periodicity.last_run = datetime.now()
        save_storage(data)


def is_paused() -> bool:
    return load_storage().paused


def toggle_pause() -> bool:
    data = load_storage()
    data.paused = not data.paused
    save_storage(data)
    return data.paused


def get_next_account_id(accounts: List[Account]) -> int:
    if not accounts:
        return 0

    return max(account.id for account in accounts) + 1
