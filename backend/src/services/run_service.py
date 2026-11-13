import logging,threading
from datetime import datetime,timedelta
from sqlalchemy import select,update
from fastapi import HTTPException
from src.config.database import SessionLocal
from src.models import AssociationStudy,ExposureStratum,TableCount,AssociationRun
from src.config.queue import LEASE_SECONDS,RENEW_SECONDS,enqueue,claim,renew,release
from src.constants.enums import RunStatus
from src.utils.audit import record
from src.utils.serializers import serialize
from .analysis_algorithm import analyze
from .errors import TaskError
from .cmh_algorithm import validate_tables

def listing(db):
 rows=[]
 for x in db.scalars(select(AssociationRun).order_by(AssociationRun.id.desc())):
  r=serialize(x);r.pop('result');r.pop('input_snapshot');rows.append(r)
 return rows

def snapshot(db,id):
 study=db.get(AssociationStudy,id)
 if not study:raise HTTPException(404,'研究不存在')
 strata=db.scalars(select(ExposureStratum).where(ExposureStratum.study_id==id).order_by(ExposureStratum.code)).all()
 if not 2<=len(strata)<=8:raise HTTPException(422,'需要2..8个完整分层')
 tables=[]
 for s in strata:
  t=db.scalar(select(TableCount).where(TableCount.stratum_id==s.id))
  if not t:raise HTTPException(422,'每层必须有2×2计数')
  tables.append({**serialize(t),'code':s.code,'name':s.name,'description':s.description})
 validate_tables(tables);return {'study':serialize(study),'tables':tables}

def create(db,data,user):
 if db.scalar(select(AssociationRun).where(AssociationRun.name==data.name)):raise HTTPException(409,'研究名称重复')
 snap=snapshot(db,data.study_id)
 row=AssociationRun(**data.model_dump(),created_by=user.id,input_snapshot=snap,
                    status=RunStatus.QUEUED,attempts=0,max_attempts=3,fencing_token=0)
 db.add(row);db.flush();record(db,user,'run.queued',row.id);db.commit()
 # 投递失败不影响冻结记录：recover() 会把仍处于 queued 的任务重新投递
 enqueue(row.id)
 return serialize(row)

def _guarded_write(db,run_id,token,**values):
 """栅栏写：仅当本进程仍持有当前 fencing token 时才更新，旧进程无法覆盖新状态。"""
 return db.execute(update(AssociationRun)
  .where(AssociationRun.id==run_id,AssociationRun.fencing_token==token)
  .values(**values)).rowcount

def _requeue(db,run_id,token,reason):
 """把任务放回等待状态；计数（attempts/阈值 alpha/快照）保持不变。

 只改库并审计，真正的重新投递在 compute 的 finally 里、释放租约之后进行，
 否则持锁状态下 enqueue 会被幂等规则抑制。返回 True 表示需要重新投递。
 """
 if not _guarded_write(db,run_id,token,status=RunStatus.QUEUED,lease_owner='',
                       lease_expires_at=None,error=(reason or '')[:700],
                       last_attempt_at=datetime.utcnow()):return False
 record(db,None,'run.requeued',run_id);db.commit();return True

def _permanent_fail(db,run_id,token,reason):
 """统计上无效等确定性错误：明确失败，不再重投，页面保留最终失败原因。"""
 if not _guarded_write(db,run_id,token,status=RunStatus.FAILED,lease_owner='',
                       lease_expires_at=None,error=(reason or '任务无法完成')[:700],
                       last_attempt_at=datetime.utcnow()):return False
 record(db,None,'run.failed',run_id);db.commit();return False

def compute(db,run_id,owner):
 """处理一个任务。返回 serialize(row) 或 None（无资格/重复消息/已终结）。

 并发安全：Redis 租约保证同一时刻只有一个进程持有处理资格；所有状态写回
 都带数据库 fencing_token 条件，失去资格的旧进程不能覆盖新进程的结果。
 """
 row=db.get(AssociationRun,run_id)
 if row is None:return None
 # 幂等：已完成或已明确失败的任务，重复消息直接跳过，不重复生成结果/完成记录
 if row.status in (RunStatus.COMPLETED,RunStatus.FAILED):return serialize(row)
 # 取得处理资格与单调 fencing token；他人持有效租约时本次为重复投递
 token=claim(run_id,owner)
 if token is None:return None
 # 双重检查（拿锁后状态可能已被别的进程改变）
 db.refresh(row)
 if row.status in (RunStatus.COMPLETED,RunStatus.FAILED):
  release(run_id,token);return serialize(row)
 # 续租线程：进程存活期间持续证明处理资格，并同步数据库租约到期时刻
 stop=threading.Event()
 def _heartbeat():
  while not stop.wait(RENEW_SECONDS):
   if not renew(run_id,owner,token):stop.set();return
   # 独立会话写心跳，避免与主处理线程共享 SQLAlchemy 会话
   try:
    with SessionLocal() as hb_db:
     hb_db.execute(update(AssociationRun).where(
       AssociationRun.id==run_id,AssociationRun.fencing_token==token)
       .values(lease_expires_at=datetime.utcnow()+timedelta(seconds=LEASE_SECONDS)))
     hb_db.commit()
   except Exception:
    logging.exception('lease heartbeat update failed run %s',run_id)
 hb=threading.Thread(target=_heartbeat,name=f'lease-{run_id}',daemon=True);hb.start()
 should_requeue=False
 try:
  # 每次真正接手 attempts+1；首次=1，页面恢复次数=attempts-1
  row.attempts=(row.attempts or 0)+1
  row.status=RunStatus.RUNNING;row.lease_owner=owner
  row.lease_expires_at=datetime.utcnow()+timedelta(seconds=LEASE_SECONDS)
  row.last_attempt_at=datetime.utcnow();row.fencing_token=token
  db.commit()
  # 计算只读取冻结时的快照与首次冻结的 alpha，续跑不改变计数与阈值
  result=analyze(row.input_snapshot,row.alpha)
  updated=_guarded_write(db,run_id,token,result=result,status=RunStatus.COMPLETED,
                         error='',lease_owner='',lease_expires_at=None,
                         fencing_token=token,last_attempt_at=datetime.utcnow())
  db.commit()
  if not updated:
   # 资格已被更新的进程接管：旧结果丢弃，绝不覆盖，也不写完成记录
   logging.warning('run %s token %s stale; result discarded',run_id,token)
   return None
  record(db,None,'worker.completed',run_id);db.commit()
  db.refresh(row);return serialize(row)
 except TaskError as exc:
  db.rollback()
  # 确定性错误（无有效统计量/超资源/快照无效）：明确失败，不浪费重试
  if exc.permanent:_permanent_fail(db,run_id,token,str(exc))
  else:should_requeue=_handle_transient(db,row,token,exc)
 except Exception as exc:
  db.rollback()
  should_requeue=_handle_transient(db,row,token,exc)
 finally:
  stop.set();release(run_id,token)
 # 租约释放后再投递，保证等待任务能立即被其他进程接手（幂等，重复安全）
 if should_requeue:enqueue(run_id)
 return None

def _handle_transient(db,row,token,exc):
 """瞬时故障：未达上限则回到等待（返回 True 由调用方释放锁后重投）。"""
 reason=f'{type(exc).__name__}: {exc}'
 if row.attempts>=row.max_attempts:
  _permanent_fail(db,row.id,token,f'重试{row.attempts-1}次后仍失败：{reason}');return False
 return _requeue(db,row.id,token,reason)

def recover(db):
 """启动/周期恢复：把失去进程的任务重新排队。

 - running 但租约已过期（worker 被杀死/崩溃）：置回 queued 并重新投递；
 - queued 但队列中没有待处理消息（Redis 被清空/投递失败）：补投。
 已完成/已失败任务保持原结果与失败记录，不重算。
 """
 now=datetime.utcnow();recovered=0
 stale=db.scalars(select(AssociationRun).where(AssociationRun.status==RunStatus.RUNNING)).all()
 for row in stale:
  if row.lease_expires_at is not None and row.lease_expires_at>now:continue
  row.status=RunStatus.QUEUED;row.lease_owner='';row.lease_expires_at=None
  row.error='处理进程中断，等待自动续跑'
  record(db,None,'run.recovered',row.id);recovered+=1
 if recovered:db.commit()
 for row in db.scalars(select(AssociationRun).where(AssociationRun.status==RunStatus.QUEUED)):
  if enqueue(row.id):recovered+=1
 return recovered
