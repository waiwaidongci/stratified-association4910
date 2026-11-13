import time,uuid,logging
from pathlib import Path
from src.config.database import SessionLocal
from src.config.queue import queue,QUEUE
from src.services.run_service import compute,recover
logging.basicConfig(level=logging.INFO)
# 每个 worker 进程一个稳定身份；多进程并发接手时据此区分处理资格
OWNER='worker-'+uuid.uuid4().hex[:12]
# 周期性清扫间隔：租约 30s，每 15s 检查一次可在约一个租约周期内接手中断任务
SWEEP_SECONDS=15
def main():
 # 启动恢复：上次崩溃留下的 running 任务重新排队，丢失的 queued 消息补投
 with SessionLocal() as db:
  try:n=recover(db);logging.info('startup recovery requeued %s run(s)',n)
  except Exception:logging.exception('startup recovery failed')
 last_sweep=time.time()
 while True:
  Path('/tmp/heartbeat').touch()
  try:
   if time.time()-last_sweep>=SWEEP_SECONDS:
    last_sweep=time.time()
    with SessionLocal() as db:
     try:recover(db)
     except Exception:logging.exception('periodic recovery failed')
   item=queue.blpop(QUEUE,timeout=4)
   if not item:continue
   run_id=int(item[1])
   with SessionLocal() as db:
    # compute 内部完成租约获取、栅栏写回、重试/失败/重投与幂等跳过
    compute(db,run_id,OWNER)
  except Exception:
   logging.exception('worker loop');time.sleep(2)
if __name__=='__main__':main()
