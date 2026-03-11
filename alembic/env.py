from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool

from backend.db_url import normalize_sqlalchemy_database_url


config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None

def _get_database_url() -> str:
    url = normalize_sqlalchemy_database_url(os.getenv("DATABASE_URL", ""))
    if not url:
        raise RuntimeError("DATABASE_URL is required for Alembic migrations")
    return url


def run_migrations_offline() -> None:
    context.configure(
        url=_get_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = create_engine(_get_database_url(), poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
