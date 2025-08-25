# alembic/versions/b77cd5aae126_create_weather_and_sales_tables.py

from alembic import op
import sqlalchemy as sa
from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = 'b77cd5aae126'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create the weather_data table
    op.create_table(
        'weather_data',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('city', sa.String(100), nullable=False),
        sa.Column('temperature_celsius', sa.Float),
        sa.Column('humidity_percent', sa.Float),
        sa.Column('wind_speed_kph', sa.Float),
        sa.Column('weather_description', sa.String(255)),
        sa.Column('timestamp_utc', sa.TIMESTAMP)
    )

    # Create the sales table
    op.create_table(
        'sales',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('product_name', sa.String(255), nullable=False),
        sa.Column('quantity', sa.Integer),
        sa.Column('price', sa.Float),
        sa.Column('sale_date', sa.Date)
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Drop the tables if you need to roll back
    op.drop_table('weather_data')
    op.drop_table('sales')