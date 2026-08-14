from .F_Server import AsyncFloroldingServer
from .F_Client import AsyncFloroldingClient
from . import Scaffolding
from threading import Thread
from typing import Callable
from copy import deepcopy
import easytier_pyo3
import asyncio
import socket
import json


class Florolding:
    def __init__(
        self,
        launcher_info: str = "EuoraCraft-Launcher",
        log_callback: Callable[[str, str], None] = print
    ):
        """
        :param launcher_info: 启动器信息
        """
        self.vendor = f"{launcher_info} (Florolding 1.0) / EasyTier {easytier_pyo3.version()} for EasyTier-PyO3"
        self.log_callback = log_callback
        self.nodes = []
        self.easytier_config = {
            "dhcp": True,
            "tcp_whitelist": ["0"],
            "udp_whitelist": ["0"],
            "flags": {
                "no_tun": True,
                "bind_device": False,
                "latency_first": True,
                "enable_kcp_proxy": True
            }
        }

    def set_nodes(self, nodes: list[str]) -> None:
        """
        设置 EasyTier 节点列表
        :param nodes: 节点列表 ["URI"]
        :return: None
        """
        self.nodes = [{"uri": node} for node in nodes]

    def create_room(
        self,
        player_name: str,
        minecraft_port: int = 25565
    ) -> tuple[str, AsyncFloroldingServer, easytier_pyo3.Node]:
        room_code = Scaffolding.generate_code()
        print(room_code)
        machine_id = Scaffolding.machine_id()
        easytier_config: dict = deepcopy(self.easytier_config)
        server_port = find_free_port()
        easytier_config.update({
            "hostname": f"scaffolding-mc-server-{server_port}",
            "network_identity": {
                "network_name": f"scaffolding-mc-{room_code[2:11]}",
                "network_secret": room_code[12:21]
            },
            "peer": self.nodes,
            "tcp_whitelist": [
                str(server_port),
                str(minecraft_port)
            ]
        })
        easytier_node = easytier_pyo3.Node(easytier_config)
        easytier_node.start()
        server = AsyncFloroldingServer(machine_id, easytier_node.peer_id(), player_name, server_port=server_port, minecraft_port=minecraft_port, vendor=self.vendor)
        Thread(target=asyncio.run, args=(server.start(),), daemon=True).start()
        return room_code, server, easytier_node


def find_free_port() -> int:
    """创建一个临时socket，绑定到端口0，让操作系统分配一个空闲端口"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # 绑定到端口0，操作系统会分配一个临时端口
        s.bind(('', 0))
        # 获取实际分配到的端口号
        port = s.getsockname()[1]
        return port
