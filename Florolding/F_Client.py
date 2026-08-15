from typing import Callable
import asyncio
import struct
import json


class AsyncFloroldingClient:
    def __init__(
        self,
        machine_id: str,
        easytier_id: str | int,
        player_name: str,
        server_host: str = "127.0.0.1",
        server_port: int = 3939,
        vendor: str = "Florolding",
        log_callback: Callable[[str, str], None] = lambda l, m: print(f"[{l}] {m}")
    ):
        """
        实例化一个异步联机客户端
        :param machine_id: 机器码(其实是什么无所谓, 别和别人的重复就行:)
        :param easytier_id: EasyTier
        :param player_name: 玩家昵称
        :param server_host: 联机大厅服务端 IP
        :param server_port: 联机大厅服务端端口
        :param vendor: 客户端信息
        :param log_callback: 日志回调, 接受一个这样的函数 (level: str, message: str) -> None
        """
        self.player_name = player_name
        self.machine_id = machine_id
        self.easytier_id = easytier_id
        self.vendor = vendor
        self.server_host = server_host
        self.server_port = server_port
        self.log_callback = log_callback
        self.reader = None
        self.writer = None

        # 支持的协议列表
        self.supported_protocols = [
            "c:ping",
            "c:protocols",
            "c:server_port",
            "c:player_easytier_id",
            "c:player_ping",
            "c:player_profiles_list"
        ]

        self.heartbeat_task = None
        self.error_num = 0

        self.loop = None

    async def c_ping(self, data: bytes = b"ping") -> bool:
        status, response_body = await self.send_request("c:ping", data)
        print(f"状态: {status}")
        print(f"请求数据: {data}")
        print(f"响应数据: {response_body}")
        if status == 0 and response_body == data:
            return True
        return False

    def sync_c_ping(self, data: bytes = b"ping") -> bool:
        return asyncio.run_coroutine_threadsafe(self.c_ping(data), self.loop).result()

    async def c_protocols(self):
        request_body = "\0".join(self.supported_protocols).encode("ascii")
        status, response_body = await self.send_request("c:protocols", request_body)
        print(f"状态: {status}")
        print(f"客户端协议: {self.supported_protocols}")
        if status == 0:
            server_protocols = response_body.decode("ascii").split("\0")
            print(f"服务器协议: {server_protocols}")
            print(f"共同协议: {set(self.supported_protocols) & set(server_protocols)}")
            print("✓ 协议协商成功")
        else:
            print(f"错误: {response_body.decode('utf-8')}")
            print("✗ 协议协商失败")

    def sync_c_protocols(self):
        if self.loop:
            asyncio.run_coroutine_threadsafe(self.c_protocols(), self.loop).result()
            return 111
        return None

    async def c_server_port(self):
        status, response_body = await self.send_request("c:server_port", b"")
        print(f"状态: {status}")
        if status == 0:
            try:
                port = struct.unpack(">H", response_body)[0]
                print(f"Minecraft服务器端口: {port}")
                print("✓ 服务器端口请求成功")
                return int(port)
            except struct.error as e:
                self.error_num += 1
                print("请求体长度:", len(response_body), "异常:", e)
        elif status == 32:
            print("服务器未启动")
        else:
            print(f"错误: {response_body.decode('utf-8')}")
            print("✗ 服务器端口请求失败")

    def sync_server_port(self):
        if self.loop:
            return asyncio.run_coroutine_threadsafe(self.c_server_port(), self.loop).result()

    async def c_player_profiles_list(self):
        status, response_body = await self.send_request("c:player_profiles_list", b"")
        print(f"状态: {status}")
        if status == 0:
            try:
                players = json.loads(response_body.decode("utf-8"))
                print(f"在线玩家 ({len(players)}人):")
                for player in players:
                    print(player)
                print("✓ 玩家列表请求成功")
            except json.decoder.JSONDecodeError:
                print("Json解析出错, 源数据:", response_body)
        else:
            print(f"错误: {response_body.decode('utf-8')}")
            print("✗ 玩家列表请求失败")

    async def start_heartbeat(self, interval: int = 5):
        """定时发送心跳"""
        async def heartbeat_loop():
            while True:
                try:
                    player_data = {
                        "name": self.player_name,
                        "machine_id": self.machine_id,
                        "easytier_id": self.easytier_id,
                        "vendor": self.vendor
                    }
                    request_body = json.dumps(player_data).encode("utf-8")
                    await self.send_request("c:player_ping", request_body)
                    print(f"[{self.player_name}] 心跳发送成功")
                    await asyncio.sleep(interval)
                except Exception as e:
                    print(f"[{self.player_name}] 心跳发送失败: {e}")
                    #break

        self.heartbeat_task = asyncio.create_task(heartbeat_loop())
        print(f"[{self.player_name}] 开始定时心跳，间隔: {interval}秒")

    @staticmethod
    def _create_request(protocol_type: str, request_body: bytes = b'') -> bytes:
        """创建TCP请求"""
        # 编码协议类型
        protocol_bytes = protocol_type.encode("ascii")
        # 构建请求: [类型长度(1字节)][类型][请求体长度(4字节)][请求体]
        request = struct.pack(">B", len(protocol_bytes))  # 类型长度
        request += protocol_bytes  # 类型
        request += struct.pack(">I", len(request_body))  # 请求体长度
        request += request_body  # 请求体
        return request

    @staticmethod
    def _parse_response(data: bytes) -> tuple:
        """解析TCP响应"""
        print("包长度:", len(data))
        if len(data) < 5:
            raise ValueError("响应数据太短")
        status = struct.unpack(">B", data[0:1])[0]
        body_length = struct.unpack(">I", data[1:5])[0]
        if len(data) < 5 + body_length:
            raise ValueError("响应体长度不匹配")
        response_body = data[5:5 + body_length]
        return status, response_body

    async def connect(self):
        """连接到基于Scaffolding协议的服务器"""
        self.reader, self.writer = await asyncio.open_connection(
            self.server_host, self.server_port
        )
        self.loop = asyncio.get_running_loop()
        print(f"已连接到服务器 {self.server_host}:{self.server_port}")
        await self.start_heartbeat()

    async def disconnect(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.reader = None
            self.writer = None
            print("已断开与服务器的连接")

    async def _serve_forever(self):
        """
        保活: 保持事件循环运行直到连接关闭
        防止 asyncio.run 在 connect 返回后取消心跳任务并销毁事件循环
        """
        while self.writer and not self.writer.is_closing():
            await asyncio.sleep(1)

    def start(self):
        """
        线程入口: 连接服务器并保活事件循环
        用法: Thread(target=client.start, daemon=True).start()
        """
        async def _run():
            await self.connect()
            await self._serve_forever()
        asyncio.run(_run())

    async def send_request(self, protocol_type: str, request_body: bytes = b"") -> tuple:
        """发送请求并接收响应"""
        if not self.writer:
            raise RuntimeError("未连接到服务器")
        # 创建并发送请求
        self.writer.write(self._create_request(protocol_type, request_body))
        await self.writer.drain()
        # 接收响应
        response = await self.reader.read(4096)
        # 正常解析响应
        return self._parse_response(response)

    async def __aenter__(self):
        """进入异步上下文连接服务器"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """退出异步上下文断开连接"""
        await self.disconnect()
