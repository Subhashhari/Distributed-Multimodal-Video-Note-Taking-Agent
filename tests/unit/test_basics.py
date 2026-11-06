import pytest

from notetaker.common.config import Settings
from notetaker.common.crypto import TokenCipher
from notetaker.common.faults import validate_faults
from notetaker.common.states import (
    STAGES,
    Status,
    can_transition,
    next_stage,
    prev_status,
    stage_progress,
)
from notetaker.common.youtube_urls import InvalidInput, normalize_notion_id, parse_youtube_url
from notetaker.worker.retry import pick_delay

VID = "dQw4w9WgXcQ"


# ----------------------------------------------------------------- URL parsing
@pytest.mark.parametrize(
    "url",
    [
        f"https://www.youtube.com/watch?v={VID}",
        f"https://youtube.com/watch?v={VID}&t=42s",
        f"https://m.youtube.com/watch?feature=share&v={VID}",
        f"youtube.com/watch?v={VID}",
        f"https://youtu.be/{VID}",
        f"https://youtu.be/{VID}?si=abc",
        f"https://www.youtube.com/shorts/{VID}",
        f"https://www.youtube.com/embed/{VID}",
        f"https://www.youtube.com/live/{VID}",
        f"https://www.youtube-nocookie.com/embed/{VID}",
    ],
)
def test_parse_valid_urls(url):
    assert parse_youtube_url(url) == VID


@pytest.mark.parametrize(
    ("url", "code"),
    [
        ("", "INVALID_YOUTUBE_URL"),
        ("not a url", "INVALID_YOUTUBE_URL"),
        ("https://www.youtube.com/watch?v=short", "INVALID_YOUTUBE_URL"),
        ("https://www.youtube.com/watch", "INVALID_YOUTUBE_URL"),
        ("https://www.youtube.com/channel/UC123", "INVALID_YOUTUBE_URL"),
        (f"ftp://youtube.com/watch?v={VID}", "INVALID_YOUTUBE_URL"),
        (f"https://vimeo.com/{VID}", "UNSUPPORTED_HOST"),
        (f"https://evil-youtube.com/watch?v={VID}", "UNSUPPORTED_HOST"),
    ],
)
def test_parse_invalid_urls(url, code):
    with pytest.raises(InvalidInput) as exc:
        parse_youtube_url(url)
    assert exc.value.code == code


def test_notion_id_normalization():
    expected = "1429989f-e8ac-4eff-bc8f-57f56486db54"
    assert normalize_notion_id("1429989fe8ac4effbc8f57f56486db54") == expected
    assert normalize_notion_id(expected) == expected
    assert normalize_notion_id("https://www.notion.so/My-Notes-1429989fe8ac4effbc8f57f56486db54") == expected
    with pytest.raises(InvalidInput):
        normalize_notion_id("hello")


# ----------------------------------------------------------------- state machine
def test_happy_path_transitions_allowed():
    path = [None, Status.QUEUED, Status.TRANSCRIBING, Status.SUMMARIZING, Status.EXTRACTING_FRAMES,
            Status.PUBLISHING, Status.COMPLETED]
    for a, b in zip(path, path[1:], strict=False):
        assert can_transition(a, b)


def test_illegal_transitions_rejected():
    assert not can_transition(Status.QUEUED, Status.SUMMARIZING)  # skipping a stage
    assert not can_transition(Status.SUMMARIZING, Status.TRANSCRIBING)  # going backwards
    assert not can_transition(Status.COMPLETED, Status.FAILED)  # terminal is final
    assert not can_transition(Status.TRANSCRIBING, Status.COMPLETED)


def test_any_non_terminal_can_fail_or_dead_letter():
    for s in (Status.QUEUED, Status.TRANSCRIBING, Status.SUMMARIZING, Status.EXTRACTING_FRAMES,
              Status.PUBLISHING):
        assert can_transition(s, Status.FAILED)
        assert can_transition(s, Status.DEAD_LETTERED)


def test_stage_helpers():
    assert prev_status("transcribe") == Status.QUEUED
    assert prev_status("frames") == Status.SUMMARIZING
    assert [next_stage(s) for s in STAGES] == ["summarize", "frames", "publish", None]
    assert stage_progress("summarize", 0) == 20
    assert stage_progress("summarize", 1) == 55
    assert stage_progress("frames", 2.0) == 85


# ----------------------------------------------------------------- backoff
def test_pick_delay_exponential_then_capped():
    s = [10, 30, 90]
    assert [pick_delay(s, n) for n in (1, 2, 3, 4, 9)] == [10, 30, 90, 90, 90]


def test_pick_delay_honors_retry_after():
    s = [10, 30, 90]
    assert pick_delay(s, 1, retry_after=12) == 30
    assert pick_delay(s, 1, retry_after=5) == 10
    assert pick_delay(s, 1, retry_after=500) == 90


def test_demo_profile_schedule():
    st = Settings(_env_file=None, retry_profile="demo", demo_backoff_schedule_sec="2,4,8")
    assert st.backoff_schedule == [2, 4, 8]


def test_worker_stages_setting():
    assert Settings(_env_file=None, worker_stages="all").stages == list(STAGES)
    assert Settings(_env_file=None, worker_stages="frames,publish").stages == ["frames", "publish"]
    with pytest.raises(ValueError):
        _ = Settings(_env_file=None, worker_stages="bogus").stages


# ----------------------------------------------------------------- crypto & faults
def test_token_roundtrip(cipher):
    enc = cipher.encrypt("ntn_secret")
    assert "ntn_secret" not in enc
    assert cipher.decrypt(enc) == "ntn_secret"


def test_cipher_rejects_placeholder():
    with pytest.raises(ValueError):
        TokenCipher("__FILL_ME_GENERATE_A_KEY__")


def test_validate_faults():
    assert validate_faults({"llm_429": 2, "frame_fail_rate": 0.5}) == {"llm_429": "2", "frame_fail_rate": "0.5"}
    with pytest.raises(ValueError):
        validate_faults({"nope": 1})
    with pytest.raises(ValueError):
        validate_faults({"frame_fail_rate": 2})


async def test_counter_fault_fires_n_times(faults, redis):
    await redis.hset("faults", "llm_429", 2)
    assert [await faults.take("llm_429") for _ in range(4)] == [True, True, False, False]
