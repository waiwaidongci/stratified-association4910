class TaskError(Exception):
    """任务处理错误。

    permanent=True 表示错误由冻结输入本身决定（如无有效统计量、超资源限额、
    快照损坏），重投任何次数都会复现，必须在达到尝试上限前就明确失败；
    permanent=False 表示瞬时故障（数据库/Redis 抖动、worker 被杀死等），
    可在剩余尝试次数内自动续跑。
    """

    def __init__(self, message, permanent=False):
        super().__init__(message)
        self.permanent = permanent
