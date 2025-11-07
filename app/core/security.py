# app/core/security.py
from datetime import datetime, timedelta
from typing import Any, Optional, Union
from argon2 import PasswordHasher  # НОВОЕ
from argon2.exceptions import VerifyMismatchError, InvalidHash  # НОВОЕ
from jose import JWTError, jwt

from app.core.config import settings

# Password hashing - ИЗМЕНЕНО с bcrypt на Argon2
ph = PasswordHasher(
    time_cost=2,  # Количество итераций
    memory_cost=65536,  # 64 MB памяти
    parallelism=1,  # Количество потоков
    hash_len=32,  # Длина хеша
    salt_len=16  # Длина соли
)

# JWT settings
ALGORITHM = settings.JWT_ALGORITHM
SECRET_KEY = settings.JWT_SECRET_KEY
ACCESS_TOKEN_EXPIRE_MINUTES = settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES


def create_access_token(
        subject: Union[str, Any],
        expires_delta: Optional[timedelta] = None
) -> str:
    """
    Create a JWT access token
    """
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode = {"exp": expire, "sub": str(subject)}
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def decode_access_token(token: str) -> Optional[dict]:
    """
    Decode a JWT access token
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verify a plain password against a hashed password
    ИЗМЕНЕНО: теперь использует Argon2 вместо bcrypt
    """
    try:
        ph.verify(hashed_password, plain_password)
        return True
    except (VerifyMismatchError, InvalidHash):
        return False


def get_password_hash(password: str) -> str:
    """
    Hash a password
    ИЗМЕНЕНО: теперь использует Argon2 вместо bcrypt
    """
    return ph.hash(password)
