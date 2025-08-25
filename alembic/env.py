# alembic/env.py

from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# --- START OF CUSTOM CODE ---
# We will use these imports to set up the connection
import os
from sqlalchemy import create_engine
# --- END OF CUSTOM CODE ---


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = None

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

# --- START OF CUSTOM CODE ---
# This code block sets up the database connection dynamically.
# This is what you must replace.

# Get connection details from environment variables or hardcoded for now
# We will use the same connection details as your Airflow connection
host = os.getenv('DB_HOST', 'postgres')
port = os.getenv('DB_PORT', '5432')
user = os.getenv('DB_USER', 'postgres')
password = os.getenv('DB_PASSWORD', 'postgres')
dbname = os.getenv('DB_NAME', 'weather_sales')

# Build the database connection URL
db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

# Set the SQLAlchemy URL in the Alembic context
context.configure(url=db_url)

# --- END OF CUSTOM CODE ---

# NOTE: The rest of the file is original Alembic code. Do not change it.
def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # --- START OF CUSTOM CODE ---
    # We will use the URL we defined earlier to create the connection
    connectable = create_engine(db_url)
    # --- END OF CUSTOM CODE ---

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()