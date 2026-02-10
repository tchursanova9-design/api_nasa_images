import time
import psycopg2
from psycopg2 import OperationalError, InterfaceError

from config import DB_NAME, DB_HOST, DB_USER, DB_PASSWORD, DB_PORT


def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME,
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        connect_timeout=5,  # важно при проблемах сети/рестарте PG
    )


def _is_transient_db_error(e: Exception) -> bool:
    # Типовые временные проблемы: Postgres перезапускается, сеть, разрыв соединения
    return isinstance(e, (OperationalError, InterfaceError))


def ensure_table(retries: int = 10) -> None:
    last_exc = None

    for attempt in range(1, retries + 1):
        conn = None
        cur = None
        try:
            conn = get_conn()
            cur = conn.cursor()

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS images (
                    id SERIAL PRIMARY KEY,
                    nasa_id TEXT NOT NULL UNIQUE,
                    image_url TEXT NOT NULL,
                    image_bytes BYTEA,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                """
            )

            conn.commit()
            return

        except Exception as e:
            last_exc = e
            # если временно — подождём и попробуем снова
            if _is_transient_db_error(e):
                sleep_s = min(2 ** attempt, 10)
                print(f"[DB] ensure_table transient error, retry in {sleep_s}s "
                      f"(attempt {attempt}/{retries}): {e}")
                time.sleep(sleep_s)
                continue

            # не временная — это баг/SQL и т.п.
            raise

        finally:
            try:
                if cur:
                    cur.close()
            except Exception:
                pass
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    # исчерпали ретраи
    raise RuntimeError(f"ensure_table failed after retries: {last_exc}")


def insert_image(nasa_id: str, image_url: str, image_bytes: bytes, retries: int = 5) -> bool:
    """
    Пытается вставить новую картинку.
    - True  -> запись реально добавлена
    - False -> дубликат (nasa_id уже есть), ничего не меняли

    На временных ошибках БД ретраит (Postgres рестарт/сеть).
    """
    last_exc = None

    for attempt in range(1, retries + 1):
        conn = None
        cur = None
        try:
            conn = get_conn()
            cur = conn.cursor()

            cur.execute(
                """
                INSERT INTO images (nasa_id, image_url, image_bytes)
                VALUES (%s, %s, %s)
                ON CONFLICT (nasa_id) DO NOTHING
                """,
                (nasa_id, image_url, psycopg2.Binary(image_bytes)),
            )

            inserted = (cur.rowcount == 1)
            conn.commit()
            return inserted

        except Exception as e:
            last_exc = e

            # временно недоступна БД -> не падаем, пробуем снова
            if _is_transient_db_error(e):
                try:
                    if conn:
                        conn.rollback()
                except Exception:
                    pass

                sleep_s = min(2 ** attempt, 10)
                print(f"[DB] transient error, retry in {sleep_s}s "
                      f"(attempt {attempt}/{retries}): {e}")
                time.sleep(sleep_s)
                continue

            # не временная -> пробрасываем
            raise

        finally:
            try:
                if cur:
                    cur.close()
            except Exception:
                pass
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    # исчерпали ретраи — пробрасываем как transient, чтобы consumer мог НЕ commit-ить
    raise last_exc
