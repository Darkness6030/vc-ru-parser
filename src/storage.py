import os
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
    last_url: Optional[str] = None
    is_blocked: bool = False


class Periodicity(BaseModel):
    interval: int
    time: time


class RegularParsingSettings(BaseModel):
    enabled: bool = False
    periodicity: Optional[Periodicity] = None
    last_run: Optional[datetime] = None


class MonitorAccountsSettings(BaseModel):
    enabled: bool = False
    periodicity: int = 0
    url_change_enabled: bool = False
    blocking_enabled: bool = False
    dtf_enabled: bool = False
    vc_enabled: bool = False
    tenchat_enabled: bool = False
    last_run: Optional[datetime] = None


class MonitorPostsSettings(BaseModel):
    enabled: bool = False
    periodicity: List[time] = []
    dtf_enabled: bool = False
    vc_enabled: bool = False
    tenchat_enabled: bool = False
    last_run: Optional[datetime] = None


class StorageData(BaseModel):
    accounts: List[Account] = []
    last_failed_accounts: List[Account] = []
    regular_parsing: RegularParsingSettings = RegularParsingSettings()
    monitor_accounts: MonitorAccountsSettings = MonitorAccountsSettings()
    monitor_posts: MonitorPostsSettings = MonitorPostsSettings()


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
    return next((account for account in get_accounts() if account.id == account_id), None)


def add_account(**kwargs):
    storage_data = load_storage()
    storage_data.accounts.append(Account(id=get_next_account_id(storage_data.accounts), **kwargs))
    save_storage(storage_data)


def update_account(account_id: int, **kwargs):
    storage_data = load_storage()
    for account in storage_data.accounts:
        if account.id == account_id:
            account.__dict__.update(**kwargs)
            break
    save_storage(storage_data)


def delete_account(account_id: int):
    storage_data = load_storage()
    storage_data.accounts = [account for account in storage_data.accounts if account.id != account_id]
    save_storage(storage_data)


def get_next_account_id(accounts: List[Account]) -> int:
    return max((account.id for account in accounts), default=0) + 1


def get_last_failed_accounts() -> List[Account]:
    return load_storage().last_failed_accounts


def set_last_failed_accounts(last_failed_accounts: List[Account]):
    storage_data = load_storage()
    storage_data.last_failed_accounts = last_failed_accounts
    save_storage(storage_data)


def get_regular_parsing_settings() -> RegularParsingSettings:
    return load_storage().regular_parsing


def toggle_regular_parsing() -> bool:
    storage_data = load_storage()
    storage_data.regular_parsing.enabled = not storage_data.regular_parsing.enabled
    save_storage(storage_data)
    return storage_data.regular_parsing.enabled


def get_regular_parsing_periodicity() -> Optional[Periodicity]:
    return load_storage().regular_parsing.periodicity


def set_regular_parsing_periodicity(interval: int, time: time):
    storage_data = load_storage()
    storage_data.regular_parsing.periodicity = Periodicity(interval=interval, time=time)
    save_storage(storage_data)


def update_regular_parsing_last_run():
    storage_data = load_storage()
    storage_data.regular_parsing.last_run = datetime.now()
    save_storage(storage_data)


def set_monitor_accounts_settings(settings: MonitorAccountsSettings):
    storage_data = load_storage()
    storage_data.monitor_accounts = settings
    save_storage(storage_data)


def get_monitor_accounts_settings() -> MonitorAccountsSettings:
    return load_storage().monitor_accounts


def toggle_monitor_accounts() -> bool:
    storage_data = load_storage()
    storage_data.monitor_accounts.enabled = not storage_data.monitor_accounts.enabled
    save_storage(storage_data)
    return storage_data.monitor_accounts.enabled


def update_monitor_accounts_last_run():
    storage_data = load_storage()
    storage_data.monitor_accounts.last_run = datetime.now()
    save_storage(storage_data)


def set_monitor_posts_settings(settings: MonitorPostsSettings):
    storage_data = load_storage()
    storage_data.monitor_posts = settings
    save_storage(storage_data)


def get_monitor_posts_settings() -> MonitorPostsSettings:
    return load_storage().monitor_posts


def toggle_monitor_posts() -> bool:
    storage_data = load_storage()
    storage_data.monitor_posts.enabled = not storage_data.monitor_posts.enabled
    save_storage(storage_data)
    return storage_data.monitor_posts.enabled


def update_monitor_posts_last_run():
    storage_data = load_storage()
    storage_data.monitor_posts.last_run = datetime.now()
    save_storage(storage_data)
