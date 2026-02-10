# observability.py
import os
import sentry_sdk


def init_sentry(service_name: str) -> None:
    dsn = os.getenv("SENTRY_DSN", "")
    env = os.getenv("ENVIRONMENT", "local")

    if not dsn:
        print("[Sentry] DSN not set, Sentry disabled")
        return

    sentry_sdk.init(
        dsn=dsn,
        environment=env,
        release=service_name,     # будет видно: producer / consumer
        traces_sample_rate=0.0,   # трассировки пока не включаем
    )

    print(f"[Sentry] enabled for {service_name} ({env})")
