import jwt
from app.core.config import settings
from datetime import timedelta, datetime, timezone
import uuid
import logging
from app.utils.setup_logging import setup_logging

logger = setup_logging(__name__)


def encode_jwt(
    payload: dict,
    private_key: str = settings.auth_jwt.private_key_path.read_text(),
    algorithm: str = settings.auth_jwt.algorithm,
    expire_timedelta: timedelta | None = None,
):
    to_encode = payload.copy()

    to_encode.update(jti=str(uuid.uuid4()))

    encoded = jwt.encode(to_encode, private_key, algorithm=algorithm)

    return encoded


def decode_jwt(
    token: str | bytes,
    public_key: str = settings.auth_jwt.public_key_path.read_text(),
    algorithm: str = settings.auth_jwt.algorithm,
) -> dict:
    decoded = jwt.decode(token, public_key, algorithms=[algorithm])
    return decoded