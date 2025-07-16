"""add reader mode tables only

Revision ID: 8539ccd0406d
Revises: 23ab4bcce866
Create Date: 2025-07-08 21:11:14.017661

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "8539ccd0406d"
down_revision: Union[str, Sequence[str], None] = "23ab4bcce866"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
