"""Encrypts the user's Notion token while it travels inside queue messages.

The token is never written to the database or logs; it exists in plaintext only in memory.
"""

from __future__ import annotations

from cryptography.fernet import Fernet, InvalidToken

KEY_HELP = (
    "MESSAGE_ENC_KEY is missing or invalid. Generate one with: "
    'python -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())"'
)


class TokenCipher:
    def __init__(self, key: str) -> None:
        if not key or key.startswith("__FILL_ME"):
            raise ValueError(KEY_HELP)
        try:
            self._fernet = Fernet(key.encode())
        except (ValueError, TypeError) as exc:
            raise ValueError(KEY_HELP) from exc

    def encrypt(self, plaintext: str) -> str:
        return self._fernet.encrypt(plaintext.encode()).decode()

    def decrypt(self, token: str) -> str:
        try:
            return self._fernet.decrypt(token.encode()).decode()
        except InvalidToken as exc:
            raise ValueError("Could not decrypt message token (MESSAGE_ENC_KEY mismatch?)") from exc
