import logging
import redis
from .settings import REDIS_URL
queue=redis.Redis.from_url(REDIS_URL,decode_responses=True)
QUEUE_NAME='cenan:calibration'
# 实际承载 run id 的列表键（worker.py 沿用该键 blpop）
QUEUE='cenan:runs'
# 已投递集合：同一 run 在队列里只保留一条待处理消息，重复投递不重复计算
PENDING_SET='cenan:runs:pending'
# 租约锁键前缀，值为 fencing_token（全局单调递增）
LOCK_PREFIX='cenan:runs:lock:'
# 租约 30 秒，由 worker 每 10 秒续租；进程死亡后租约到期即可被其他进程接手
LEASE_SECONDS=30
RENEW_SECONDS=10
# 全局栅栏令牌计数器
TOKEN_COUNTER='cenan:runs:fencing'

def _lock_key(run_id):return f'{LOCK_PREFIX}{run_id}'

def enqueue(run_id):
    """幂等投递：已在队列等待或仍被有效租约持有的 run 不会重复入队。"""
    try:
        if queue.get(_lock_key(run_id)) is not None:return False
        if not queue.sadd(PENDING_SET,run_id):return False  # 已有待处理消息
        queue.rpush(QUEUE,run_id)
        return True
    except redis.RedisError:
        logging.exception('enqueue run %s failed',run_id)
        return False

def claim(run_id,owner):
    """原子取得 run 的处理资格，返回单调递增 fencing_token；竞争失败返回 None。"""
    key=_lock_key(run_id)
    try:
        token=queue.incr(TOKEN_COUNTER)
        # setnx：仅当无有效租约时占位，保证多进程并发下只有一个赢家
        if not queue.set(key,token,nx=True,ex=LEASE_SECONDS):
            return None
        queue.srem(PENDING_SET,run_id)
        return token
    except redis.RedisError:
        logging.exception('claim run %s failed',run_id)
        return None

def renew(run_id,owner,token):
    """续租并校验处理资格；令牌不匹配（已被更新的进程接手）时返回 False。"""
    key=_lock_key(run_id)
    try:
        current=queue.get(key)
        if current is None or int(current)!=token:return False
        queue.expire(key,LEASE_SECONDS)
        return True
    except redis.RedisError:
        logging.exception('renew run %s failed',run_id)
        return False

def release(run_id,token):
    """仅允许持有者本人释放，旧进程不能清掉新进程的租约。"""
    key=_lock_key(run_id)
    try:
        current=queue.get(key)
        if current is not None and int(current)==token:
            queue.delete(key);return True
        return False
    except redis.RedisError:
        logging.exception('release run %s failed',run_id)
        return False
