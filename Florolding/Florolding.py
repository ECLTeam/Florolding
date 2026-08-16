from .F_Server import AsyncFloroldingServer
from .F_Client import AsyncFloroldingClient
from . import Scaffolding
from ipaddress import IPv4Address
from threading import Thread
from typing import Callable
from copy import deepcopy
import easytier_pyo3
import asyncio
import socket
import time


class Florolding:
    def __init__(
        self,
        launcher_info: str = "EuoraCraft-Launcher",
        log_callback: Callable[[str, str], None] = lambda l, m: print(f"[{l}] {m}")
    ):
        """
        :param launcher_info: 启动器信息
        :param log_callback: 日志回调, 接受一个这样的函数 (level: str, message: str) -> None
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
                "multi_thread": True,
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
        """
        创建一个房间(注意一定要管理好 EasyTier 节点的生命周期!!!!!)
        :param player_name: 玩家昵称
        :param minecraft_port: Minecraft 端口
        :return: (房间码, 服务端实例, EasyTierPyo3Node)
        """
        room_code = Scaffolding.generate_code()
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

        server = AsyncFloroldingServer(
            machine_id=machine_id,
            easytier_id=easytier_node.peer_id(),
            player_name=player_name,
            server_port=server_port,
            minecraft_port=minecraft_port,
            vendor=self.vendor,
            log_callback=self.log_callback
        )
        Thread(target=asyncio.run, args=(server.start(),), daemon=True).start()

        return room_code, server, easytier_node

    def join_room(
        self,
        room_code: str,
        player_name: str,
        conn_timeout: int = 30
    ) -> tuple[AsyncFloroldingClient, easytier_pyo3.Node]:
        if not Scaffolding.validate_code(room_code):
            raise ValueError("房间码不合规")

        machine_id = Scaffolding.machine_id()
        easytier_config: dict = deepcopy(self.easytier_config)
        easytier_config.update({
            "network_identity": {
                "network_name": f"scaffolding-mc-{room_code[2:11]}",
                "network_secret": room_code[12:21]
            },
            "peer": self.nodes
        })

        easytier_node = easytier_pyo3.Node(easytier_config)
        easytier_node.start()

        host_node_info = self.get_host_node_info(easytier_node, conn_timeout)

        server_port = host_node_info["hostname"].replace("scaffolding-mc-server-", "")
        if not server_port.isnumeric() or not (0 < int(server_port) <= 65535):
            easytier_node.stop()
            raise ValueError("联机大厅端口不合规")

        host_virtual_ip = IPv4Address(host_node_info['ipv4_addr']['address']['addr']).exploded
        bind_server_port = find_free_port()
        easytier_node.apply_config({
            "port_forward": [
                {
                    "bind_addr": f"127.0.0.1:{bind_server_port}",
                    "dst_addr": f"{host_virtual_ip}:{server_port}",
                    "proto": "tcp"
                }
            ]
        })

        client = AsyncFloroldingClient(
            machine_id=machine_id,
            easytier_id=easytier_node.peer_id(),
            player_name=player_name,
            server_port=bind_server_port,
            vendor=self.vendor,
            log_callback=self.log_callback
        )
        Thread(target=client.start, daemon=True).start()
        while not client.writer:
            time.sleep(1)

        # 马上协商协议, 防止某些 Scaffolding 实现会炸
        client.sync_c_protocols()

        return client, easytier_node

    @staticmethod
    def get_host_node_info(
        easytier_node: easytier_pyo3.Node,
        conn_timeout: int = 30
    ):
        host_node_info = {}
        start_time = time.time()
        while True:
            for node_info in easytier_node.routes():
                if node_info["hostname"].lower().startswith("scaffolding-mc-server-") and (
                        node_info.get("ipv4_addr", {}).get("address", {}).get("addr")
                ):
                    host_node_info = node_info
                    break
            if host_node_info:
                break
            time.sleep(1)
            if time.time() - start_time > conn_timeout:
                easytier_node.stop()
                raise TimeoutError("连接超时, 无法找到联机大厅")
        return host_node_info

    def bind_mc_port(
        self,
        client: AsyncFloroldingClient,
        easytier_node: easytier_pyo3.Node,
        bind_mc_port: int = 25565,
        timeout: int = 30
    ) -> bool:
        mc_port = client.sync_c_server_port()
        if not mc_port:
            return False

        host_node_info = self.get_host_node_info(easytier_node, timeout)
        host_virtual_ip = IPv4Address(host_node_info['ipv4_addr']['address']['addr']).exploded
        easytier_node.apply_config({
            "port_forward": [
                {
                    "bind_addr": f"127.0.0.1:{bind_mc_port}",
                    "dst_addr": f"{host_virtual_ip}:{mc_port}",
                    "proto": "tcp"
                }
            ]
        })

        return True


def find_free_port() -> int:
    """创建一个临时socket，绑定到端口0，让操作系统分配一个空闲端口"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # 绑定到端口0，操作系统会分配一个临时端口
        s.bind(('', 0))
        # 获取实际分配到的端口号
        port = s.getsockname()[1]
        return port
