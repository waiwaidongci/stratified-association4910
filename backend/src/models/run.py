from datetime import datetime
from sqlalchemy import String,ForeignKey,Double,JSON,DateTime,Integer,UniqueConstraint
from sqlalchemy.orm import Mapped,mapped_column
from src.config.database import Base
class AssociationRun(Base):
 __tablename__='association_runs'
 id:Mapped[int]=mapped_column(primary_key=True)
 name:Mapped[str]=mapped_column(String(160),unique=True)
 study_id:Mapped[int]=mapped_column(ForeignKey('association_studies.id'))
 alpha:Mapped[float]=mapped_column(Double(asdecimal=False),default=.05)
 status:Mapped[str]=mapped_column(String(20),default='queued')
 input_snapshot:Mapped[dict]=mapped_column(JSON,default=dict)
 result:Mapped[dict]=mapped_column(JSON,default=dict)
 error:Mapped[str]=mapped_column(String(700),default='')
 # 中断恢复相关：attempts 为实际接手处理的次数（首次接手后=1），恢复次数=attempts-1；
 # lease_owner/lease_expires_at 记录当前处理资格与租约到期时刻；
 # fencing_token 单调递增，旧进程持有的令牌落后时无法写回结果。
 attempts:Mapped[int]=mapped_column(Integer,default=0)
 max_attempts:Mapped[int]=mapped_column(Integer,default=3)
 lease_owner:Mapped[str]=mapped_column(String(64),default='')
 lease_expires_at:Mapped[datetime]=mapped_column(DateTime,nullable=True)
 fencing_token:Mapped[int]=mapped_column(Integer,default=0)
 last_attempt_at:Mapped[datetime]=mapped_column(DateTime,nullable=True)
 created_by:Mapped[int]=mapped_column(ForeignKey('users.id'))
 created_at:Mapped[datetime]=mapped_column(DateTime,default=datetime.utcnow)
