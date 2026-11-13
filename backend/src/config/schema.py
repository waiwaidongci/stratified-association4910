import logging
from sqlalchemy import inspect,text
from src.config.database import engine
from src.models.run import AssociationRun

# create_all 不会给已存在的表补列；演示库可能由旧版本创建，这里做最小幂等升级。
_NEW_COLUMNS={
 'attempts':'INT NOT NULL DEFAULT 0',
 'max_attempts':'INT NOT NULL DEFAULT 3',
 'lease_owner':"VARCHAR(64) NOT NULL DEFAULT ''",
 'lease_expires_at':'DATETIME NULL',
 'fencing_token':'INT NOT NULL DEFAULT 0',
 'last_attempt_at':'DATETIME NULL',
}
def ensure_run_columns():
 inspector=inspect(engine)
 if AssociationRun.__tablename__ not in inspector.get_table_names():return
 existing={c['name'] for c in inspector.get_columns(AssociationRun.__tablename__)}
 with engine.begin() as conn:
  for name,ddl in _NEW_COLUMNS.items():
   if name not in existing:
    conn.execute(text(f'ALTER TABLE {AssociationRun.__tablename__} ADD COLUMN {name} {ddl}'))
    logging.info('added column %s.%s',AssociationRun.__tablename__,name)
