## 汐声(TideEcho)——一个简单易用的C++网络库

### 说明

本项目是一个轻量级C++网络库，需要 **C++20** 或以上标准，目前仅提供 TCP 相关服务，支持 **Windows** 和 **POSIX**（Linux/macOS 等）。

包含两个头文件：`TideEcho.h`（平台无关接口）和 `TideEchoNative.h`（平台转换工具），以及一个实现文件 `TideEcho.cpp`。

### 使用说明

将 `TideEcho.h`、`TideEchoNative.h`、`TideEcho.cpp` 下载到项目中，设置头文件所在目录为附加包含目录，并将 `TideEcho.cpp` 一同编译即可。

**链接库**（Windows 需显式链接）：

- Windows：`ws2_32.lib`、`iphlpapi.lib`
- POSIX：无需额外库（系统标准库自动链接）

完整使用示例可参考随库提供的 `example.cpp`。

### 核心API介绍

#### 1. 初始化和清理

```c++
bool Initialize();
void Cleanup();
```



在 Windows 下初始化 Winsock；POSIX 下为空操作。推荐使用 RAII 包装类 `NetServiceGuard`。

```c++
class NetServiceGuard { ... };
```



#### 2. Socket

底层套接字包装，通常不建议直接使用。

#### 3. TCPStreamBuffer

`std::streambuf` 派生类，提供阻塞/非阻塞/超时模式的收发。

**构造函数**：

- `TCPStreamBuffer(NetEndpoint remote, NetEndpoint local = {})`：指定远端（和可选本地），发起非阻塞连接。
- `TCPStreamBuffer(AddressFamily family)`：仅指定地址族（IPv4/IPv6），暂不连接，等待后续 `connect`。
- `TCPStreamBuffer(Socket&& sock)`：从已存在的套接字构建（用于 `accept`）。

**收发函数**：

```c++
int64_t recv(std::span<uint8_t> buffer, int64_t timeout_ms = -1);
int64_t send(std::span<const uint8_t> buffer, int64_t timeout_ms = -1);
```



- `timeout_ms = -1`：阻塞模式。
- `timeout_ms = 0`：非阻塞，立即返回。
- `timeout_ms > 0`：等待指定毫秒数，超时返回 -1。
- 返回值 >0：实际收发字节数；返回 0：对端关闭；返回 -1：失败（需检查状态）。

#### 4. TCPStream

继承自 `std::iostream`，内部持有 `TCPStreamBuffer`，大多数接口为转发。

```c++
TCPStream(NetEndpoint remote, NetEndpoint local = {});
TCPStream(AddressFamily family);
TCPStream(std::unique_ptr<TCPStreamBuffer>&& buf);
```



提供 `is_open()`、`status()`、`connect()` 以及 `recv`/`send` 等函数，用法与 `TCPStreamBuffer` 一致。

#### 5. TCPListener

监听器，支持双栈（IPv6 默认开启 `IPV6_V6ONLY=0`，可同时接受 IPv4 和 IPv6 连接）。

```c++
TCPListener(const NetEndpoint& local);     // 指定本地端点
TCPListener(uint16_t port);                // 监听 ::0:port（双栈）
std::unique_ptr<TCPStream> accept(int64_t timeout_ms = -1);
void reset(const NetEndpoint& local = {});
void close();
TCPListenerStatus status();
const NetEndpoint& local() const;          // 获取实际绑定的地址
```



#### 6. AsyncSendResult

异步发送状态查询。

```c++
enum class AsyncSendStatus { InQueue, Sending, Sended, Failed };
class AsyncSendResult { ... };
```



#### 7. TCPClient

异步 TCP 客户端，需定期调用 `update()` 驱动收发（非线程安全）。

```c++
TCPClient(NetEndpoint remote, NetEndpoint local = {});
TCPClient(AddressFamily family);
TCPStreamStatus connect(NetEndpoint remote);   // 可重新连接
void update();                                 // 必须循环调用
AsyncSendResult asyncSend(std::vector<uint8_t> data);
AsyncSendResult asyncSendRef(std::span<const uint8_t> data);  // 引用数据，需保证生命周期
std::optional<std::vector<uint8_t>> getPackage(); // 获取完整数据包
```



- `asyncSend`/`asyncSendRef` 和 `getPackage` 是**线程安全**的。
- `update()` 非线程安全，需在单一线程中循环调用。

#### 8. TCPServer

异步 TCP 服务器，同样需循环调用 `update()`。

```c++
TCPServer(NetEndpoint local);
TCPServer(uint16_t port);
void update();                                 // 单线程驱动
std::vector<std::function<void()>> updateTasks(); // 返回各连接的任务，可并行执行
AsyncSendResult asyncSend(std::vector<uint8_t> data, NetEndpoint remote);
AsyncSendResult asyncSendRef(std::span<const uint8_t> data, NetEndpoint remote);
std::optional<NetPackage> getPackage();
std::vector<NetEndpoint> remote() const;       // 当前所有客户端
```



- `updateTasks()` 先将新连接和待发送数据分发完毕，再为每个连接生成一个 `update` 任务，允许外部线程池并发处理，提高吞吐。
- 线程安全规则同 `TCPClient`。

#### 9. 辅助函数

```c++
std::vector<std::pair<AddressFamily, std::string>> GetLocalIPs();
```



获取本机所有 IPv4/IPv6 地址（用于调试或服务发现）。

### 数据包协议

库内部使用固定头部（4 字节小端长度） + 数据体，用户无需手动处理，直接通过 `getPackage` 获取完整数据包。

### 注意事项

- **必须定期调用 `update()`**：所有实际 I/O 操作都在 `update` 中完成，否则数据将积压。
- **线程安全性**：`update` 非线程安全；发送和接收队列操作（`asyncSend`、`getPackage`）是线程安全的。
- **析构时**：未发送的数据包状态将被置为 `Failed`，保证不会悬空。
- **Windows 注意**：需链接 `ws2_32` 和 `iphlpapi`。

### 开源许可证

本项目采用 [MIT License](https://opensource.org/licenses/MIT)。

```
Copyright (c) 2026 linmu-i

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

