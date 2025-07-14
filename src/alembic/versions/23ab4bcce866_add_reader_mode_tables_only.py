"""add reader mode tables only

Revision ID: 23ab4bcce866
Revises: 717347cd9873
Create Date: 2025-07-08 21:11:08.197204

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "23ab4bcce866"
down_revision: Union[str, Sequence[str], None] = "717347cd9873"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
