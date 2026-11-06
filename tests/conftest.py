from __future__ import annotations

import base64
import os

import fakeredis
import pytest

from notetaker.common.config import Settings
from notetaker.common.crypto import TokenCipher
from notetaker.common.db import Database
from notetaker.common.faults import Faults
from tests.fakes import FakeBroker

TEST_KEY = base64.urlsafe_b64encode(os.urandom(32)).decode()
NOTION_TOKEN = "ntn_test_token_0123456789abcdef"


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'test.db'}",
        message_enc_key=TEST_KEY,
        admin_token="test-admin-token",
        demo_faults_enabled=True,
        stub_stage_delay_sec=0,
        auto_create_schema=True,
        max_attempts=5,
        rl_max_retries=3,
        backoff_schedule_sec="10,30,90",
    )


@pytest.fixture
async def db(settings):
    database = Database(settings.database_url)
    await database.create_all()
    yield database
    await database.dispose()


@pytest.fixture
def broker() -> FakeBroker:
    return FakeBroker()


@pytest.fixture
def redis():
    return fakeredis.FakeAsyncRedis(decode_responses=True)


@pytest.fixture
def cipher(settings) -> TokenCipher:
    return TokenCipher(settings.message_enc_key)


@pytest.fixture
def faults(redis, settings) -> Faults:
    return Faults(redis, settings.demo_faults_enabled)
