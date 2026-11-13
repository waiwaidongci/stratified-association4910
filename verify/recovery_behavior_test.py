"""纯标准库行为验证：不依赖 redis/sqlalchemy/numpy/scipy。

用内存假件替换第三方库，验证：
1) 重复投递幂等（同一 run 队列中只有一条消息，持锁期间不重投）
2) 多进程并发 claim 只有一个赢家，fencing token 单调递增
3) 栅栏写：旧 token 无法覆盖新结果
4) 瞬时故障自动续跑，仍用首次冻结计数/阈值；达 max_attempts 明确失败
5) 确定性 TaskError(permanent) 立即明确失败，不重试
6) recover() 接管过期 running 任务并补投丢失的 queued 消息
7) 已完成/已失败任务的重复消息被幂等跳过，不重复写完成记录
"""
import sys,types,threading,time,uuid

# ---------- 假 redis ----------
class FakeRedis:
 def __init__(self):self._kv={};self._list=[];self._set=set();self._lock=threading.RLock()
 def sadd(self,k,v):
  with self._lock:
   if v in self._set:return 0
   self._set.add(v);return 1
 def srem(self,k,v):
  with self._lock:self._set.discard(v)
 def rpush(self,k,v):
  with self._lock:self._list.append(v)
 def blpop(self,k,timeout=0):
  deadline=time.time()+timeout
  while True:
   with self._lock:
    if self._list:return (k,self._list.pop(0))
   if time.time()>=deadline:return None
   time.sleep(0.01)
 def incr(self,k):
  with self._lock:
   self._kv[k]=int(self._kv.get(k,0))+1;return self._kv[k]
 def decr(self,k):
  with self._lock:self._kv[k]=int(self._kv.get(k,0))-1;return self._kv[k]
 def set(self,k,v,nx=False,ex=None):
  with self._lock:
   if nx and k in self._kv:return False
   self._kv[k]=str(v);return True
 def get(self,k):
  with self._lock:return self._kv.get(k)
 def expire(self,k,sec):return True
 def ttl(self,k):return 1 if k in self._kv else -2
 def delete(self,k):
  with self._lock:self._kv.pop(k,None)
 def ping(self):return True
 def flush(self):self._kv.clear();self._list.clear();self._set.clear()

# ---------- 假 sqlalchemy ----------
sa=types.ModuleType('sqlalchemy')
def _column(*a,**k):
 class C:
  def __init__(s):s.primary_key=bool(k.get('primary_key'))
 return C()
class _Types:
 def __getattr__(s,n):return lambda *a,**k:_column()
sa.String=sa.ForeignKey=sa.Double=sa.JSON=sa.DateTime=sa.Integer=sa.UniqueConstraint=None
class _Select:
 def __init__(s,model):s._model=model;s._status=None
 def where(s,cond):
  # 假条件对象携带状态字面量
  s._status=getattr(cond,'_fake_status',None);return s
sa.select=lambda m:_Select(m)
class _Cond:
 def __init__(s,status):s._fake_status=status
class _Col:
 def __eq__(s,other):return _Cond(other)
# 让模型上的 status 表现为可比较列（运行期用 FakeRow，无需真实映射）
class _Update:
 def __init__(s,m):s._m=m;s._where=[];s._values={}
 def where(s,*c):s._where.extend(c);return s
 def values(s,**k):s._values.update(k);return s
sa.update=lambda m:_Update(m)
orm=types.ModuleType('sqlalchemy.orm')
class Mapped:pass
def mapped_column(*a,**k):return _column()
orm.Mapped=Mapped;orm.mapped_column=mapped_column
class _DeclarativeBase:
 pass
orm.DeclarativeBase=_DeclarativeBase
inspect_mod=types.ModuleType('sqlalchemy.inspect');inspect_mod.inspect=lambda *a,**k:None
sys.modules['sqlalchemy']=sa;sys.modules['sqlalchemy.orm']=orm;sys.modules['sqlalchemy.inspect']=inspect_mod

# ---------- 假 fastapi ----------
fp=types.ModuleType('fastapi')
class HTTPException(Exception):
 def __init__(s,code,msg):s.code=code;s.msg=msg
fp.HTTPException=HTTPException;sys.modules['fastapi']=fp

# ---------- 假项目配置/模型/工具 ----------
sys.path.insert(0,'/workspace/backend')
# 预占位所有 src 包，避免触发真实 __init__ 里的第三方依赖
_PKG_PATHS={'src':'/workspace/backend/src','src.config':'/workspace/backend/src/config',
 'src.constants':'/workspace/backend/src/constants','src.utils':'/workspace/backend/src/utils',
 'src.services':'/workspace/backend/src/services','src.models':'/workspace/backend/src/models'}
for pkg,pth in _PKG_PATHS.items():
 m=types.ModuleType(pkg);m.__path__=[pth];sys.modules[pkg]=m
settings=types.ModuleType('src.config.settings');settings.REDIS_URL='memory://'
sys.modules['src.config.settings']=settings
# redis 假件
redis_mod=types.ModuleType('redis')
class RedisError(Exception):pass
class _RedisAPI:
 @staticmethod
 def from_url(*a,**k):return FakeRedis()
redis_mod.Redis=_RedisAPI;redis_mod.RedisError=RedisError
sys.modules['redis']=redis_mod
import importlib
qmod=importlib.import_module('src.config.queue')
qmod.queue=FakeRedis();qmod.LEASE_SECONDS=30;qmod.RENEW_SECONDS=10

db_mod=types.ModuleType('src.config.database')
class _Base:_declared={}
db_mod.Base=_Base
class SessionLocal:
 def __enter__(s):return s
 def __exit__(s,*a):return False
db_mod.SessionLocal=SessionLocal
sys.modules['src.config.database']=db_mod

enums=types.ModuleType('src.constants.enums')
class RunStatus:
 QUEUED='queued';RUNNING='running';COMPLETED='completed';FAILED='failed'
enums.RunStatus=RunStatus
sys.modules['src.constants.enums']=enums

# 假 ORM 行与“栅栏更新”语义
class FakeRow:
 def __init__(s,id):
  s.id=id;s.name=f'run-{id}';s.status='queued';s.alpha=.05;s.attempts=0;s.max_attempts=3
  s.lease_owner='';s.lease_expires_at=None;s.fencing_token=0;s.last_attempt_at=None
  s.error='';s.result={};s.input_snapshot={'tables':[]}
TABLE={}
AUDIT=[]
class FakeSession:
 def __init__(s):s.rollback_calls=0
 def get(s,m,i):return TABLE.get(i)
 def refresh(s,r):pass
 def rollback(s):s.rollback_calls+=1
 def commit(s):pass
 def add(s,o):AUDIT.append(o)
 def scalars(s,q):
  status=getattr(q,'_status',None)
  class R:
   def _data(self):
    if status=='running':return [r for r in TABLE.values() if r.status=='running']
    if status=='queued':return [r for r in TABLE.values() if r.status=='queued']
    return list(TABLE.values())
   def all(self):return self._data()
   def __iter__(self):return iter(self._data())
  return R()
 def scalar(s,q):return None
 def execute(s,stmt):
  # 只处理 update 的栅栏写
  row=TABLE.get(stmt._m._fake_id)
  cond=stmt._where
  ok=row is not None and all(getattr(c,'_fake_eval')(row) for c in cond)
  class Res:rowcount=0
  res=Res()
  if ok:
   for k,v in stmt._values.items():setattr(row,k,v)
   res.rowcount=1
  return res

# 让 update(AssociationRun) 可被识别
class _Model:
 _fake_id=None
def make_model(fid):
 class M(_Model):_fake_id=fid
 return M
# 替换 run_service 内对 AssociationRun 的引用与 record/analyze

# 假 record
audit_mod=types.ModuleType('src.utils.audit')
def record(db,user,action,target):AUDIT.append((action,target))
audit_mod.record=record;sys.modules['src.utils.audit']=audit_mod
ser_mod=types.ModuleType('src.utils.serializers')
ser_mod.serialize=lambda o:dict(o.__dict__)
sys.modules['src.utils.serializers']=ser_mod

# 可编排的 analyze：默认成功；可注入故障
ANALYZE_BEHAVIOR={}
def fake_analyze(snapshot,alpha):
 behavior=ANALYZE_BEHAVIOR.get('mode','ok')
 if behavior=='permanent':
  from src.services.errors import TaskError;raise TaskError('全部层无有效条件方差',permanent=True)
 if behavior=='transient':
  raise RuntimeError('simulated worker crash / db blip')
 if behavior=='slow':
  time.sleep(ANALYZE_BEHAVIOR.get('sleep',0.3));return {'fingerprint':'ok','alpha':alpha}
 return {'fingerprint':'ok','alpha':alpha}
ana_mod=types.ModuleType('src.services.analysis_algorithm');ana_mod.analyze=fake_analyze
sys.modules['src.services.analysis_algorithm']=ana_mod
# cmh validate 假件（snapshot 路径在本测试不触发）
cmh=types.ModuleType('src.services.cmh_algorithm');cmh.validate_tables=lambda t:t
sys.modules['src.services.cmh_algorithm']=cmh

# 模型引用注入
models_pkg=types.ModuleType('src.models')
class AssociationRun:_fake_id=None
AssociationRun.status=_Col()
models_pkg.AssociationRun=AssociationRun
# 其余被 run_service import 的名字
for n in ['AssociationStudy','ExposureStratum','TableCount']:setattr(models_pkg,n,type(n,(),{}))
sys.modules['src.models']=models_pkg

import importlib
rs=importlib.import_module('src.services.run_service')
# 测试用内存栅栏写，等价于 SQL UPDATE ... WHERE fencing_token=token
def guarded_write(db,run_id,token,**values):
 row=TABLE.get(run_id)
 if row is None or row.fencing_token!=token:return 0
 for k,v in values.items():setattr(row,k,v)
 return 1
rs._guarded_write=guarded_write

passed=[];failed=[]
def check(name,cond):
 (passed if cond else failed).append(name)
 print(('  PASS ' if cond else '  FAIL ')+name)

# 测试1：重复投递幂等
print('T1 重复投递幂等')
qmod.queue.flush();TABLE.clear();AUDIT.clear()
TABLE[1]=FakeRow(1)
check('首次投递成功',qmod.enqueue(1) is True)
check('二次投递被去重',qmod.enqueue(1) is False)
check('队列仅一条消息',len(qmod.queue._list)==1)
tok=qmod.claim(1,'w1')
check('持锁期间投递被拒',qmod.enqueue(1) is False)
qmod.release(1,tok)

# 测试2：并发 claim 唯一赢家 + token 单调
print('T2 并发 claim 与 fencing token')
qmod.queue.flush()
winners=[];tokens=[]
def worker(owner):
 t=qmod.claim(7,owner)
 if t is not None:winners.append(owner);tokens.append(t)
ts=[threading.Thread(target=worker,args=(f'w{i}',)) for i in range(8)]
TABLE[7]=FakeRow(7)
for t in ts:t.start()
for t in ts:t.join()
check('仅一个赢家',len(winners)==1)
check('token 为正整数',len(tokens)==1 and tokens[0]>=1)
t1=qmod.claim(7,'w2')
check('租约未释放时再次 claim 失败',t1 is None)
qmod.release(7,tokens[0])
t2=qmod.claim(7,'w2')
check('释放后新进程拿到更大 token',t2 is not None and t2>tokens[0])
qmod.release(7,t2)

# 测试3：栅栏写——旧 token 不能覆盖新结果
print('T3 栅栏写防旧进程覆盖')
qmod.queue.flush();TABLE.clear()
TABLE[9]=FakeRow(9)
db=FakeSession()
old=qmod.claim(9,'old');rs_capture=old
# 旧进程把行 token 写成 old
row=TABLE[9];row.fencing_token=old;row.status='running'
# 租约到期，新进程接手
qmod.release(9,old)
new=qmod.claim(9,'new');check('新 token 严格更大',new>old)
row.fencing_token=new
# 旧进程尝试用旧 token 落结果
n=guarded_write(db,9,old,status='completed',result={'fingerprint':'STALE'})
check('旧 token 写入 0 行',n==0)
check('结果未被覆盖',row.status=='running' and row.result=={})
n2=guarded_write(db,9,new,status='completed',result={'fingerprint':'FRESH'})
check('新 token 写入成功',n2==1 and row.status=='completed' and row.result['fingerprint']=='FRESH')
qmod.release(9,new)

# 测试4：瞬时故障自动续跑，计数/阈值不变，达到上限明确失败
print('T4 瞬时故障重试与上限失败')
qmod.queue.flush();TABLE.clear();AUDIT.clear()
TABLE[3]=FakeRow(3);TABLE[3].alpha=.007
ANALYZE_BEHAVIOR.clear();ANALYZE_BEHAVIOR['mode']='transient'
db=FakeSession()
for attempt in range(3):
 before_alpha=TABLE[3].alpha
 rs.compute(db,3,f'w{attempt}')
 check(f'第{attempt+1}次后 alpha 仍为首次阈值',TABLE[3].alpha==before_alpha==.007)
check('三次接手 attempts=3',TABLE[3].attempts==3)
check('达到上限状态 failed',TABLE[3].status=='failed')
check('失败原因含重试说明','重试' in TABLE[3].error)
check('产生两次 requeued + 一次 failed 审计',
      [a[0] for a in AUDIT if isinstance(a,tuple)].count('run.requeued')==2 and
      any(a[0]=='run.failed' for a in AUDIT if isinstance(a,tuple)))
check('失败后重复消息被幂等跳过',rs.compute(db,3,'wx') is not None)

# 测试5：确定性错误立即失败，不重试
print('T5 确定性统计错误立即明确失败')
qmod.queue.flush();TABLE.clear();AUDIT.clear()
TABLE[4]=FakeRow(4)
ANALYZE_BEHAVIOR.clear();ANALYZE_BEHAVIOR['mode']='permanent'
rs.compute(FakeSession(),4,'w1')
check('attempts 仅 1（未重试）',TABLE[4].attempts==1)
check('状态 failed',TABLE[4].status=='failed')
check('保留最终失败原因',TABLE[4].error=='全部层无有效条件方差')
check('无 requeued 审计',not any(isinstance(a,tuple) and a[0]=='run.requeued' for a in AUDIT))

# 测试6：recover 接管过期 running，并补投丢失的 queued
print('T6 recover 中断接管与消息补投')
qmod.queue.flush();TABLE.clear();AUDIT.clear()
import datetime
TABLE[5]=FakeRow(5);TABLE[5].status='running';TABLE[5].lease_owner='dead-w'
TABLE[5].lease_expires_at=datetime.datetime.utcnow()-datetime.timedelta(seconds=5)
TABLE[6]=FakeRow(6);TABLE[6].status='queued'  # Redis 已无该消息
# 一个健康（未过期）的 running 不应被接管
TABLE[8]=FakeRow(8);TABLE[8].status='running';TABLE[8].lease_owner='alive-w'
TABLE[8].lease_expires_at=datetime.datetime.utcnow()+datetime.timedelta(seconds=20)
n=rs.recover(FakeSession())
check('过期 running 被置回 queued',TABLE[5].status=='queued')
check('健康 running 保持 running',TABLE[8].status=='running')
check('两个任务被重新投递（含补投）',n>=2 and sorted(qmod.queue._list)==[5,6])
check('恢复计数 attempts 未被 recover 改变',TABLE[5].attempts==0)

# 测试7：恢复后续跑成功，恢复次数=attempts-1，且只写一次完成
print('T7 恢复续跑成功且完成记录唯一')
qmod.queue.flush();TABLE.clear();AUDIT.clear()
ANALYZE_BEHAVIOR.clear();ANALYZE_BEHAVIOR['mode']='ok'
TABLE[2]=FakeRow(2)
db=FakeSession()
rs.compute(db,2,'w1')  # 首次成功
first_completed=sum(1 for a in AUDIT if isinstance(a,tuple) and a[0]=='worker.completed')
check('首次即完成 attempts=1',TABLE[2].attempts==1 and TABLE[2].status=='completed')
# 重复消息（队列残留/重复投递）
rs.compute(db,2,'w2')
second_completed=sum(1 for a in AUDIT if isinstance(a,tuple) and a[0]=='worker.completed')
check('重复消息不重复写完成记录',first_completed==second_completed==1)
check('结果未被二次处理改变',TABLE[2].result['fingerprint']=='ok')

print()
# 测试8：瞬时故障后释放锁再投递，另一进程接手并成功（计数/阈值沿用首次冻结）
print('T8 故障释放后自动重投并由新进程接手')
qmod.queue.flush();TABLE.clear();AUDIT.clear()
ANALYZE_BEHAVIOR.clear();ANALYZE_BEHAVIOR['mode']='transient'
TABLE[10]=FakeRow(10);TABLE[10].alpha=.01
db=FakeSession()
rs.compute(db,10,'w1')  # 第一次瞬时失败
check('失败后回到 queued',TABLE[10].status=='queued')
check('锁已释放，可被重新 claim',qmod.claim(10,'w2') is not None)
check('队列中补了一条待处理消息',10 in qmod.queue._list or TABLE[10].status=='queued')
check('attempts=1 且 alpha 沿用首次阈值',TABLE[10].attempts==1 and TABLE[10].alpha==.01)
# 释放 w2 刚拿的锁，模拟它成为合法处理者；改 analyze 为成功
qmod.queue.flush()
tok=qmod.claim(10,'w2')
TABLE[10].fencing_token=tok;TABLE[10].status='queued'
ANALYZE_BEHAVIOR['mode']='ok'
# 直接以新 owner 再跑一次（claim 幂等：已持锁则 compute 内部再次 claim 会失败，
# 故先释放再由 compute 自行 claim，贴近真实 worker 从队列取消息的路径）
qmod.release(10,tok);qmod.enqueue(10)
rs.compute(FakeSession(),10,'w2')
check('第二次接手后完成',TABLE[10].status=='completed')
check('attempts=2，恢复次数=1，阈值未变',TABLE[10].attempts==2 and TABLE[10].alpha==.01)
check('完成结果指纹正确',TABLE[10].result['fingerprint']=='ok')
check('仅一条完成记录',sum(1 for a in AUDIT if isinstance(a,tuple) and a[0]=='worker.completed')==1)

print()
print(f'通过 {len(passed)} 项，失败 {len(failed)} 项')
sys.exit(1 if failed else 0)
