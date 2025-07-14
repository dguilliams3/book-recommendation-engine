"""add_reader_mode_tables_only

Revision ID: 15f7bcd9a862
Revises: 8539ccd0406d
Create Date: 2025-07-08 21:11:18.074289

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "15f7bcd9a862"
down_revision: Union[str, Sequence[str], None] = "8539ccd0406d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
