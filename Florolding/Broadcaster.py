from collections.abc import Callable
from mcstatus import JavaServer
import threading
import socket
import time

class MinecraftLANBroadcaster:
    """
    持续检测 Minecraft 服务器，在线时自动向局域网广播。
    广播内容包含动态 MOTD（含玩家数量），离线时停止广播。
    """
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 25565,
        broadcast_interval=3,
        check_interval=5,
        log_callback: Callable[[str, str], None] = lambda l, m: print(f"[{l}] {m}")
    ):
        """
        :param host: 目标服务器 IP 或域名
        :param port: 目标服务器端口
        :param broadcast_interval: 广播包发送间隔（秒），一般 1.5~3
        :param check_interval: 状态检查间隔（秒），检测服务器是否在线
        :param log_callback: 日志回调, 接受一个这样的函数 (level: str, message: str) -> None
        """
        self.host = host
        self.port = port
        self.broadcast_interval = broadcast_interval
        self.check_interval = check_interval
        self.log_callback = log_callback

        # 广播相关
        self.broadcast_addr = "255.255.255.255"  # 可改为子网广播地址，如 "192.168.1.255"
        self.broadcast_port = 4445
        self._broadcasting = False
        self._stop_event = threading.Event()

        # 共享状态（由监控线程更新，广播线程读取）
        self._server_online = False
        self._motd = "Minecraft Server"
        self._player_count = 0
        self._max_players = 0
        self._lock = threading.Lock()

        # 线程
        self._monitor_thread = None
        self._broadcast_thread = None

    def _update_status(self):
        """使用 mcstatus 查询服务器信息，更新共享状态"""
        try:
            server = JavaServer.lookup(f"{self.host}:{self.port}")
            status = server.status()
            with self._lock:
                self._server_online = True
                # 提取 MOTD（支持字符串或 Chat 对象）
                motd = status.description
                if isinstance(motd, dict):
                    motd = motd.get("text", str(motd))
                elif motd is None:
                    motd = "Minecraft Server"
                # 动态添加玩家数量（逼真）
                self._motd = f"{motd} [{status.players.online}/{status.players.max}]"
                self._player_count = status.players.online
                self._max_players = status.players.max
        except Exception:
            with self._lock:
                self._server_online = False

    def _broadcast_loop(self):
        """广播线程主循环，持续发送 UDP 包直到收到停止信号"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        while not self._stop_event.is_set():
            with self._lock:
                if not self._server_online:
                    # 若服务器离线，直接休眠并继续循环（但实际广播线程会在监控线程中停止）
                    # 此处为了安全，如果离线则停止发送
                    time.sleep(self.broadcast_interval)
                    continue
                motd = self._motd
                port = self.port

            # 构造标准 LAN 发现数据包
            message = f"[MOTD]{motd}[/MOTD][AD]{port}[/AD]"
            data = message.encode("utf-8")

            try:
                sock.sendto(data, (self.broadcast_addr, self.broadcast_port))
                # 可选：输出调试信息
                # self.log_callback("INFO", f"广播: {message}")
            except Exception as e:
                self.log_callback("ERROR", f"广播发送失败: {e}")

            # 等待下一次广播
            time.sleep(self.broadcast_interval)

        sock.close()
        self.log_callback("INFO", "广播线程已停止")

    def _monitor_loop(self):
        """监控线程主循环，定期检查服务器状态，控制广播线程"""
        while not self._stop_event.is_set():
            # 检查服务器状态
            self._update_status()

            with self._lock:
                online = self._server_online

            # 根据在线状态管理广播线程
            if online and not self._broadcasting:
                # 服务器在线但广播未启动 → 启动广播
                self._start_broadcaster()
            elif not online and self._broadcasting:
                # 服务器离线但广播仍在运行 → 停止广播
                self._stop_broadcaster()
            # 其他情况保持不变

            # 等待下一次检查
            time.sleep(self.check_interval)

        # 退出监控时确保停止广播
        if self._broadcasting:
            self._stop_broadcaster()
        self.log_callback("INFO", "端口监控线程已停止")

    def _start_broadcaster(self):
        """启动广播线程"""
        if self._broadcasting:
            return
        self._stop_event.clear()  # 重置停止标志
        self._broadcast_thread = threading.Thread(target=self._broadcast_loop, daemon=True)
        self._broadcast_thread.start()
        self._broadcasting = True
        self.log_callback("INFO", "广播已启动")

    def _stop_broadcaster(self):
        """停止广播线程"""
        if not self._broadcasting:
            return
        self._stop_event.set()
        if self._broadcast_thread and self._broadcast_thread.is_alive():
            self._broadcast_thread.join(timeout=2)
        self._broadcasting = False
        self.log_callback("INFO", "广播已停止")

    def start(self):
        """启动监控服务（非阻塞）"""
        if self._monitor_thread and self._monitor_thread.is_alive():
            self.log_callback("WARNING", "端口监控服务已在运行")
            return
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        self.log_callback("INFO", "端口监控服务已启动")

    def stop(self):
        """停止整个服务"""
        self._stop_event.set()
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=2)
        # 确保广播也停止
        if self._broadcasting:
            self._stop_broadcaster()
        self.log_callback("INFO", "广播服务已完全停止")

