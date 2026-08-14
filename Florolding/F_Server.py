from re import match as re_match
from typing import Callable
import asyncio
import struct
import json


class AsyncFloroldingServer:
    def __init__(
        self,
        machine_id: str,
        easytier_id: int | str,
        player_name: str,
        server_host: str = "0.0.0.0",
        server_port: int = 3939,
        minecraft_port: int = 25565,
        vendor: str = "Florolding",
        log_callback: Callable[[str, str], None] = print
    ):
        """
        实例化一个异步联机客户端
        :param machine_id: 机器码(其实是什么无所谓, 别和别人的重复就行:)
        :param easytier_id: EasyTier
        :param player_name: 玩家昵称
        :param server_host: 联机大厅服务端 IP
        :param server_port: 联机大厅服务端端口
        :param minecraft_port: Minecraft 端口
        :param vendor: 客户端信息
        """
        player_name = player_name
        self.server_host = server_host
        self.server_port = server_port
        self.minecraft_port = minecraft_port
        self.log_callback = log_callback
        self.server = None

        self.players = {
            machine_id: {
                "name": player_name,
                "machine_id": machine_id,
                "easytier_id": easytier_id,
                "vendor": vendor,
                "kind": "HOST"
            }
        }  # {machine_id: player_info}
        self.machine_ids = {}  # {writer: machine_id}

        self.lock = asyncio.Lock()  # 异步锁

        # 协议处理器映射
        self.protocol_handlers = {
            "c:ping": self.__c_ping,
            "c:protocols": self.__c_protocols,
            "c:server_port": self.__c_server_port,
            "c:player_ping": self.__c_player_ping,
            "c:player_profiles_list": self.__c_player_profiles_list
        }

        # 支持的协议列表
        self.supported_protocols = list(self.protocol_handlers.keys())
        self.supported_protocols.extend([
            "c:player_easytier_id"
        ])

        self._loop = None

    def set_minecraft_port(self, minecraft_port: int | str):
        self.minecraft_port = minecraft_port

    @staticmethod
    async def __c_ping(request_body: bytes) -> tuple:
        return 0, request_body  # 返回相同的请求体

    async def __c_protocols(self, request_body: bytes) -> tuple:
        try:
            """
            # 客户端支持的协议列表 (由\0分割)
            # 取交集，确定共同支持的协议
            common_protocols = list(set(request_body.decode("ascii").split("\0")) & set(self.supported_protocols))
            # 构建响应体
            response_body = "\0".join(common_protocols).encode("ascii")
            return 0, response_body
            """
            return 0, "\0".join(self.supported_protocols).encode("ascii")
        except UnicodeDecodeError:
            return 255, b"Invalid protocol format"

    async def __c_server_port(self, request_body: bytes) -> tuple:
        return 0, struct.pack(">H", self.minecraft_port)

    async def __c_player_ping(self, request_body: bytes, writer: asyncio.StreamWriter) -> tuple:
        try:
            # 解析JSON请求体
            player_data = json.loads(request_body.decode("utf-8"))
            if not all(field in player_data for field in ["name", "machine_id", "vendor"]):
                return 255, b"Missing required fields"
            machine_id = player_data.get("machine_id")
            async with self.lock:
                if writer not in self.machine_ids:
                    self.machine_ids.update({writer: machine_id})
                    if machine_id not in self.players:
                        player_data.update({"kind": "GUEST"})
                        self.players.update({machine_id: player_data})
            return 0, b""
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            return 255, f"Invalid JSON format: {e}".encode("utf-8")

    async def __c_player_profiles_list(self, request_body: bytes) -> tuple:
        try:
            # 构建玩家列表
            async with self.lock:
                self.log_callback("DEBUG", str(self.players))
                return 0, json.dumps(list(self.players.values())).encode("utf-8")
        except Exception as e:
            return 255, f"Error generating player list: {e}".encode("utf-8")

    async def c_player_profiles_list(self) -> list:
        try:
            # 构建玩家列表
            async with self.lock:
                return list(self.players.values())
        except Exception:
            return []

    @staticmethod
    def __parse_request(data: bytes) -> tuple:
        """解析TCP请求"""
        if len(data) < 5:  # 最小长度检查
            return None, None, 255
        try:
            # 读取请求类型长度 (uint8)
            type_length = struct.unpack(">B", data[0:1])[0]
            # 检查数据长度是否足够
            if len(data) < 1 + type_length + 4:
                return None, None, 255
            # 读取请求类型 (ASCII字符串)
            protocol_type = data[1:1 + type_length].decode("ascii")
            # 验证协议格式
            if not bool(re_match(r"^[a-z0-9_]+:[a-z0-9_]+$", protocol_type)):
                return None, None, 255
            # 读取请求体长度 (uint32)
            body_length = struct.unpack(">I", data[1 + type_length:5 + type_length])[0]
            # 检查请求体长度是否匹配
            if len(data) < 5 + type_length + body_length:
                return None, None, 255
            # 读取请求体
            request_body = data[5 + type_length:5 + type_length + body_length]
            return protocol_type, request_body, 0
        except (struct.error, UnicodeDecodeError, ValueError):
            return None, None, 255

    @staticmethod
    def __create_response(status: int, response_body: bytes = b"") -> bytes:
        """构建请求体"""
        body_length = len(response_body)
        # 构建响应: [状态(1字节)][响应体长度(4字节)][响应体]
        response = struct.pack(">B", status)  # 状态
        response += struct.pack(">I", body_length)  # 响应体长度
        response += response_body  # 响应体
        return response

    async def __remove_player(self, writer: asyncio.StreamWriter):
        """移除玩家"""
        async with self.lock:
            if writer in self.machine_ids:
                machine_id = self.machine_ids.pop(writer)
                if machine_id in self.players:
                    self.players.pop(machine_id)

    async def __handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        address = writer.get_extra_info("peername")
        self.log_callback("INFO", f"新的连接: {address}")
        try:
            while True:
                data = await reader.read(4096)
                if not data: break
                protocol_type, request_body, parse_status = self.__parse_request(data)
                if parse_status != 0:
                    # 解析错误
                    error_msg = f"Parse error: {parse_status}".encode("utf-8")
                    writer.write(self.__create_response(255, error_msg))
                    await writer.drain()
                    continue
                self.log_callback("DEBUG", f"{self.machine_ids.get(writer)} 调用: {protocol_type}")
                if protocol_type in self.protocol_handlers:
                    if protocol_type == "c:player_ping":
                        # 特殊处理玩家心跳，需要传入writer
                        status, response_body = await self.__c_player_ping(request_body, writer)
                    else:
                        status, response_body = await self.protocol_handlers[protocol_type](request_body)
                    response = self.__create_response(status, response_body)
                else:
                    # 不支持的协议
                    error_msg = f"Unsupported protocol: {protocol_type}".encode("utf-8")
                    response = self.__create_response(255, error_msg)
                # 发送响应
                self.log_callback("DEBUG", f"响应长度: {len(response)}")
                writer.write(response)
                await writer.drain()
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            # 客户端断开连接
            pass
        except Exception as e:
            self.log_callback("ERROR", f"异常: {e}")
        finally:
            # 客户端断开连接，立即移除相关玩家
            await self.__remove_player(writer)
            writer.close()
            await writer.wait_closed()
            self.log_callback("INFO", f"连接关闭: {address}")

    async def start(self):
        """启动Florolding TCP服务器, 基于Scaffolding协议"""
        self._loop = asyncio.get_running_loop()

        self.server = await asyncio.start_server(
            self.__handle_client,
            self.server_host,
            self.server_port
        )

        self.log_callback("INFO", f"FloroldingServer 启动在 {self.server_host}:{self.server_port}")
        self.log_callback("INFO", f"支持的协议: {', '.join(self.supported_protocols)}")
        self.log_callback("INFO", f"Minecraft 端口: {self.minecraft_port}")

        try:
            async with self.server:
                await self.server.serve_forever()
        except asyncio.exceptions.CancelledError:
            pass

    async def stop(self):
        """停止Florolding TCP服务器"""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        self.log_callback("INFO", "FloroldingServer 已停止")

    def sync_stop(self) -> bool:
        if self._loop:
            asyncio.run_coroutine_threadsafe(self.stop(), self._loop)
            return True
        return False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """退出异步上下文自动关闭服务器"""
        await self.stop()
