# app/auth.py
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, VerificationError, InvalidHashError
from datetime import datetime, timedelta
from jose import JWTError, jwt
from typing import Optional
from app.config import settings
import logging

logger = logging.getLogger(__name__)

# Argon2 Password Hasher с рекомендованными параметрами
ph = PasswordHasher(
    time_cost=2,        # Количество итераций
    memory_cost=65536,  # Память в КБ (64 MB)
    parallelism=4,      # Количество параллельных потоков
    hash_len=32,        # Длина хеша в байтах
    salt_len=16         # Длина соли в байтах
)

# JWT константы
SECRET_KEY = settings.JWT_SECRET_KEY
ALGORITHM = settings.JWT_ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Проверить пароль с использованием Argon2
    
    Args:
        plain_password: Открытый пароль
        hashed_password: Хешированный пароль из БД
    
    Returns:
        True если пароль совпадает, False иначе
    """
    try:
        # Argon2 автоматически проверяет пароль
        ph.verify(hashed_password, plain_password)
        
        # Проверить нужно ли обновить хеш (если параметры изменились)
        if ph.check_needs_rehash(hashed_password):
            logger.info("Password hash needs rehashing with new parameters")
        
        return True
        
    except VerifyMismatchError:
        # Пароль не совпадает
        return False
    except (VerificationError, InvalidHashError) as e:
        # Ошибка верификации или невалидный хеш
        logger.error(f"Password verification error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during password verification: {e}")
        return False


def get_password_hash(password: str) -> str:
    """
    Хешировать пароль с использованием Argon2
    
    Args:
        password: Открытый пароль (любой длины!)
    
    Returns:
        Хешированный пароль
    """
    try:
        # Argon2 не имеет ограничений на длину пароля!
        hashed = ph.hash(password)
        return hashed
    except Exception as e:
        logger.error(f"Error hashing password: {e}")
        raise ValueError(f"Failed to hash password: {e}")


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Создать JWT токен
    
    Args:
        data: Данные для включения в токен
        expires_delta: Время жизни токена
    
    Returns:
        JWT токен
    """
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    
    try:
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt
    except Exception as e:
        logger.error(f"Error creating access token: {e}")
        raise ValueError(f"Failed to create access token: {e}")


def decode_access_token(token: str) -> Optional[dict]:
    """
    Декодировать JWT токен
    
    Args:
        token: JWT токен
    
    Returns:
        Payload токена или None если токен невалиден
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError as e:
        logger.debug(f"JWT decode error: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error decoding token: {e}")
        return None


def verify_token(token: str) -> Optional[str]:
    """
    Проверить токен и вернуть username
    
    Args:
        token: JWT токен
    
    Returns:
        Username из токена или None
    """
    payload = decode_access_token(token)
    if payload is None:
        return None
    
    username: str = payload.get("sub")
    return username


def create_refresh_token(data: dict) -> str:
    """
    Создать refresh токен (опционально, для будущего использования)
    
    Args:
        data: Данные для включения в токен
    
    Returns:
        Refresh токен
    """
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=30)  # Refresh токен живёт дольше
    to_encode.update({"exp": expire, "type": "refresh"})
    
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt