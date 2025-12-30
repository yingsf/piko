import asyncio
import datetime
import os
import socket
from typing import cast

from sqlalchemy import update, select, CursorResult
from sqlalchemy.dialects.mysql import insert as mysql_insert

from piko.config import settings
from piko.infra.db import SchedulerLeader, get_session, utcnow
from piko.infra.logging import get_logger
from piko.infra.observability import LEADER_STATUS

logger = get_logger(__name__)


class LeaderMutex:
    """分布式 Leader 选举互斥锁（基于数据库租约 + CAS 乐观锁）

    在多实例调度器集群中实现 Leader 选举，确保任一时刻只有一个节点担任 Leader
    核心机制：
    - 基于数据库的租约（Lease）：Leader 持有未过期的租约
    - CAS 版本号乐观锁：防止并发抢占导致的双主问题（ABA 问题）
    - 心跳续约：Leader 定期延长租约，失败则自动降级

    Attributes:
        self._session_factory (callable): 数据库会话工厂函数（默认 get_session）
        self.owner_id (str): 当前节点标识，格式为 "hostname:pid"
        self.lease_name (str): 租约名称（通常为固定值，如 'scheduler-leader'）
        self.lease_seconds (int): 租约时长（秒），超过此时间未续约则被视为过期
        self._is_leader (bool): 内存中的 Leader 状态标志
        self._current_version (int): 内存中的版本号快照，用于 CAS 校验

    Methods:
        ensure_seed: 初始化租约表种子数据
        try_acquire: 尝试抢占或续约 Leader 锁
        extend_lease: 延长当前租约（仅 Leader 可调用）
        release: 主动释放 Leader 锁

    Warning:
        - 必须先调用 ensure_seed() 初始化表数据
        - 心跳间隔应远小于 lease_seconds，避免网络抖动导致频繁降级
        - 数据库连接故障会导致所有节点降级，需要配合数据库高可用方案
    """

    def __init__(self, session_factory=get_session):
        """初始化 Leader 选举互斥锁

        Args:
            session_factory (callable): 数据库会话工厂函数，默认为 get_session
        """
        self._session_factory = session_factory
        # 全局唯一的节点标识
        self.owner_id = f"{socket.gethostname()}:{os.getpid()}"
        # 租约名称（通常为固定值）
        self.lease_name = settings.leader_name
        # 租约时长，需大于心跳间隔
        self.lease_seconds = settings.leader_lease_s
        # 初始状态为非 Leader
        self._is_leader = False
        # 版本号快照，用于 CAS 校验
        self._current_version = 0

    @property
    def is_leader(self) -> bool:
        """检查当前节点是否为 Leader

        Returns:
            bool: True 表示当前节点持有 Leader 锁
        """
        return self._is_leader

    def _set_leader_status(self, is_leader: bool, version: int = 0):
        """统一更新内存状态和监控指标（避免状态不一致）

        Args:
            is_leader (bool): 新的 Leader 状态
            version (int): 最新版本号（仅在成为 Leader 时需要）默认为 0
        """
        self._is_leader = is_leader
        metric_val = 1 if is_leader else 0
        # 更新 Prometheus 指标，便于监控系统实时追踪 Leader 状态
        LEADER_STATUS.labels(host=self.owner_id).set(metric_val)
        if is_leader:
            # 仅在成为 Leader 时更新版本号快照
            self._current_version = version

    async def ensure_seed(self):
        """初始化租约表的种子数据（幂等操作）

        在应用启动时调用，确保 SchedulerLeader 表中存在租约记录
        使用 MySQL 的 INSERT ... ON DUPLICATE KEY UPDATE 实现幂等性

        Note:
            MySQL upsert 语义：
            - 若主键不存在，插入初始记录（owner='init', version=0）
            - 若主键已存在，仅更新 name 字段（实际不改变任何内容，用于兼容语法）

            设计考量：
            - 所有节点启动时都会调用此方法，但只有第一个成功插入
            - 后续节点的 upsert 不会覆盖已有数据，保证安全性
            - 异常时仅记录警告日志，不中断启动流程（容错设计）
        """
        # 构造 MySQL 特有的 upsert 语句
        stmt = (
            mysql_insert(SchedulerLeader)
            .values(
                name=self.lease_name,
                # 初始占位符，真正的 owner 在首次抢占时设置
                owner="init",
                # 初始租约立即过期，允许任意节点抢占
                lease_until=utcnow(),
                # 初始版本号
                version=0
            )
            .on_duplicate_key_update(name=self.lease_name)
        )
        try:
            async for session in self._session_factory():
                await session.execute(stmt)
                await session.commit()
        except Exception as e:
            logger.warning("leader_seed_failed", error=str(e))

    async def try_acquire(self) -> bool:
        """尝试抢占或续约 Leader 锁

        核心抢占逻辑：
        1. 读取当前租约状态（owner、lease_until、version）
        2. 判断是否可抢占：lease_until < now OR owner == self
        3. 若可抢占，执行 CAS 更新（WHERE version = old_version）
        4. 根据 CAS 结果更新内存状态和监控指标

        Returns:
            bool: True 表示成功抢占或续约，False 表示抢占失败（锁被其他节点持有）
        """
        now = utcnow()
        # 计算新的租约到期时间（当前时间 + 租约时长）
        new_lease_until = now + datetime.timedelta(seconds=self.lease_seconds)

        async for session in self._session_factory():
            try:
                # 委托核心抢占逻辑给辅助方法，降低复杂度
                success = await self._attempt_acquire_logic(session, now, new_lease_until)
                if not success:
                    # 抢占失败，显式降级（虽然通常已经是 False，但保证幂等性）
                    self._set_leader_status(False)
                return success
            except Exception as e:
                logger.error("leader_acquire_error", error=str(e))
                self._set_leader_status(False)
                return False

        # 理论上不会走到这里（async for 应至少执行一次），但为了满足类型检查
        return False

    async def _attempt_acquire_logic(self, session, now, new_lease_until) -> bool:
        """核心抢占逻辑：读取 -> 检查条件 -> 决定是否更新

        Args:
            session (AsyncSession): 数据库会话对象
            now (datetime): 当前 UTC 时间
            new_lease_until (datetime): 新的租约到期时间

        Returns:
            bool: True 表示可抢占且 CAS 更新成功，False 表示抢占失败
        """
        # 第一步：读取当前租约状态
        stmt_read = select(SchedulerLeader).where(SchedulerLeader.name == self.lease_name)
        result = await session.execute(stmt_read)
        row = result.scalar_one_or_none()

        if not row:
            # 租约记录不存在（种子未初始化或被误删），无法抢占
            return False

        # 第二步：判断抢占条件
        # 可抢占的两种情况：1) 锁过期  2) 我是 owner（续约）
        if row.lease_until < now or row.owner == self.owner_id:
            # 第三步：执行 CAS 更新
            return await self._perform_cas_update(session, row, now, new_lease_until)

        # 锁未过期且不是我持有，抢占失败
        return False

    async def _perform_cas_update(self, session, row, now, new_lease_until) -> bool:
        """执行 CAS (Compare-And-Swap) 更新抢占租约

        Args:
            session (AsyncSession): 数据库会话对象
            row (SchedulerLeader): 读取到的租约记录（包含 version）
            now (datetime): 当前 UTC 时间
            new_lease_until (datetime): 新的租约到期时间

        Returns:
            bool: True 表示 CAS 更新成功（成为或保持 Leader），False 表示 CAS 失败（被并发抢占）

        Note:
            CAS 机制核心：
            - WHERE 条件同时匹配 name 和 version，确保原子性
            - 若 version 被其他节点改变，rowcount 为 0，表示抢占失败
            - 成功后 version += 1，防止 ABA 问题

            并发场景示例：
            - 节点 A 和 B 同时读到 version=10
            - A 先执行更新，version 变为 11，rowcount=1
            - B 执行更新时 WHERE version=10 匹配失败，rowcount=0
            - B 的 CAS 失败，日志记录 "leader_cas_failed"
        """
        # 构造 CAS 更新语句：WHERE version = old_version
        stmt_update = (
            update(SchedulerLeader)
            .where(
                SchedulerLeader.name == self.lease_name,
                # CAS 关键条件
                SchedulerLeader.version == row.version
            )
            .values(
                # 更新为当前节点
                owner=self.owner_id,
                # 延长租约
                lease_until=new_lease_until,
                # 版本号递增，防止 ABA 问题
                version=row.version + 1,
                # 更新时间戳
                updated_at=now
            )
        )
        res = await session.execute(stmt_update)
        await session.commit()

        # 使用 cast 显式转换类型，消除 Mypy 对 rowcount 的未解析警告
        row_count = cast(CursorResult, res).rowcount

        if row_count > 0:
            # CAS 成功：我成功抢占或续约了租约
            if not self._is_leader:
                # 首次成为 Leader，记录日志（续约时不重复记录）
                logger.info("leader_acquired", owner=self.owner_id)
            # 更新内存状态和监控指标
            self._set_leader_status(True, row.version + 1)
            return True
        else:
            # CAS 失败：被其他节点并发抢占（版本号不匹配）
            logger.debug("leader_cas_failed", me=self.owner_id)
            return False

    async def extend_lease(self) -> bool:
        """延长当前租约（心跳续约）

        仅当当前节点是 Leader 时才执行续约，内部调用 try_acquire 复用抢占逻辑

        Returns:
            bool: True 表示续约成功（仍为 Leader），False 表示续约失败（已降级）

        Note:
            设计原因：
            - 续约本质上是"重新抢占"，复用 try_acquire 避免代码重复
            - 若续约失败（如数据库连接断开），try_acquire 会自动降级
        """
        if not self._is_leader:
            # 非 Leader 不应调用续约，直接返回 False
            return False
        # 复用抢占逻辑进行续约
        return await self.try_acquire()

    async def release(self) -> None:
        """主动释放 Leader 锁（优雅下线）

        在节点下线或需要主动让出 Leader 角色时调用，将租约到期时间设为当前时间

        Note:
            幂等性设计：
            - 非 Leader 调用时直接返回，不执行数据库操作
            - WHERE 条件包含 owner 校验，防止误释放其他节点的锁
            - 异常时静默失败（使用 pass），因为释放失败不影响最终一致性
              （租约自然过期后其他节点可以抢占）
        """
        if not self._is_leader:
            # 非 Leader 无需释放
            return

        async for session in self._session_factory():
            try:
                # 将租约到期时间设为当前时间，使其立即过期
                stmt = (
                    update(SchedulerLeader)
                    .where(
                        SchedulerLeader.name == self.lease_name,
                        # 仅释放自己持有的锁
                        SchedulerLeader.owner == self.owner_id
                    )
                    .values(lease_until=utcnow())
                )
                await session.execute(stmt)
                await session.commit()
                logger.info("leader_released", owner=self.owner_id)
            except Exception:
                # 释放失败不中断流程（租约会自然过期）
                pass
            finally:
                # 无论数据库操作是否成功，都更新内存状态和监控指标
                self._set_leader_status(False)


# 全局单例
_leader_mutex: LeaderMutex | None = None


def get_leader_mutex() -> LeaderMutex:
    """获取全局唯一的 LeaderMutex 实例（单例模式）

    Returns:
        LeaderMutex: 全局 Leader 选举互斥锁实例

    Note:
        单例设计原因：
        - 全进程共享同一个 Mutex 实例，避免重复初始化
        - 内存状态（_is_leader）在单例中保持一致
        - 降低资源开销（数据库连接池等）
    """
    global _leader_mutex
    if _leader_mutex is None:
        _leader_mutex = LeaderMutex()
    return _leader_mutex


class LeaderWatchdog:
    """Leader 选举看门狗（后台心跳任务）

    负责在后台持续运行心跳循环，执行以下任务：
    - Leader 节点：定期续约（调用 extend_lease）
    - Follower 节点：定期尝试抢占（调用 try_acquire）

    与 LeaderMutex 的关系：
    - Watchdog 是 Mutex 的"驱动者"，Mutex 是"状态机"
    - Watchdog 负责定时触发状态转换，Mutex 负责具体的抢占/续约逻辑

    Attributes:
        self._mutex (LeaderMutex): 关联的 Leader 选举互斥锁
        self._running (bool): 看门狗运行状态标志
        self._task (asyncio.Task | None): 后台心跳任务对象

    Methods:
        start: 启动后台心跳任务
        stop: 停止后台心跳任务
        _loop: 心跳循环的核心逻辑
    """

    def __init__(self, mutex: LeaderMutex):
        """初始化 Leader 选举看门狗

        Args:
            mutex (LeaderMutex): 关联的 Leader 选举互斥锁实例
        """
        self._mutex = mutex
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self):
        """启动后台心跳任务

        创建一个 asyncio.Task 运行 _loop 方法，实现非阻塞的后台心跳
        """
        if self._running:
            # 已启动，避免重复创建任务
            return
        self._running = True
        # 创建后台任务（非阻塞）
        self._task = asyncio.create_task(self._loop())
        # 满足 linter 要求：async 函数必须有 await 表达式
        await asyncio.sleep(0)
        logger.info("leader_watchdog_started")

    async def stop(self):
        """停止后台心跳任务（优雅关闭）"""
        self._running = False
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        logger.info("leader_watchdog_stopped")

    async def _loop(self):
        """心跳循环的核心逻辑（后台持续运行）

        根据当前节点角色执行不同操作：
        - Leader：调用 extend_lease 续约，失败则自动降级
        - Follower：调用 try_acquire 尝试抢占
        """
        while self._running:
            try:
                if self._mutex.is_leader:
                    # Leader 节点：执行续约
                    success = await self._mutex.extend_lease()
                    if not success:
                        # 续约失败，记录警告（状态已在 Mutex 内部降级）
                        logger.warning("leader_renew_failed_demoting")
                        # 无需手动设置 _is_leader=False，Mutex 已处理
                else:
                    # Follower 节点：尝试抢占
                    await self._mutex.try_acquire()
            except Exception as e:
                # 捕获所有异常，避免循环中断（例如数据库连接断开）
                logger.error("leader_watchdog_error", error=str(e))

            # 休眠指定间隔后继续下一轮心跳
            await asyncio.sleep(settings.leader_renew_interval_s)


_leader_watchdog: LeaderWatchdog | None = None


def get_leader_watchdog() -> LeaderWatchdog:
    """获取全局唯一的 LeaderWatchdog 实例（单例模式）

    Returns:
        LeaderWatchdog: 全局 Leader 选举看门狗实例
    """
    global _leader_watchdog
    if _leader_watchdog is None:
        _leader_watchdog = LeaderWatchdog(get_leader_mutex())
    return _leader_watchdog
