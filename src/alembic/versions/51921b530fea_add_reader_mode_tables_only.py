"""add_reader_mode_tables_only

Revision ID: 51921b530fea
Revises: 15f7bcd9a862
Create Date: 2025-07-08 21:11:24.045877

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "51921b530fea"
down_revision: Union[str, Sequence[str], None] = "15f7bcd9a862"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
