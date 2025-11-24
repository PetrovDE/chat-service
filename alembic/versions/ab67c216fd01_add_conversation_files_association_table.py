"""Add conversation_files association table

Revision ID: ab67c216fd01
Revises: 2e8e83f8318f
Create Date: 2025-11-24 15:57:26.371295

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'ab67c216fd01'
down_revision: Union[str, Sequence[str], None] = '2e8e83f8318f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create conversation_files association table
    op.create_table(
        'conversation_files',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('conversation_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('file_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('added_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['conversation_id'], ['conversations.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['file_id'], ['files.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('conversation_id', 'file_id', name='uq_conversation_file')
    )
    # Create indexes for better query performance
    op.create_index(op.f('ix_conversation_files_conversation_id'), 'conversation_files', ['conversation_id'])
    op.create_index(op.f('ix_conversation_files_file_id'), 'conversation_files', ['file_id'])


def downgrade() -> None:
    op.drop_index(op.f('ix_conversation_files_file_id'), table_name='conversation_files')
    op.drop_index(op.f('ix_conversation_files_conversation_id'), table_name='conversation_files')
    op.drop_table('conversation_files')
    # ### end Alembic commands ###
