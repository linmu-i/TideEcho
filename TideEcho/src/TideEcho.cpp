#include <cstdlib>
#include <chrono>
#include <bit>

#include <TideEcho.h>
#include <TideEchoNative.h>

namespace tideecho
{
	template<typename T>
	concept TrivialCopyable = std::is_trivially_copyable_v<T>;

	template<TrivialCopyable T>
	constexpr T ByteSwap(T data)
	{
		auto bytes = std::bit_cast<std::array<std::byte, sizeof(T)>>(data);
		for (size_t i = 0; i < bytes.size() / 2; ++i)
		{
			std::swap(bytes[i], bytes[bytes.size() - 1 - i]);
		}
		return std::bit_cast<T>(bytes);
	}

	template<TrivialCopyable T>
	constexpr T ToLittleEndian(T data)
	{
		if constexpr (std::endian::native == std::endian::big)
		{
			return ByteSwap(data);
		}
		else
		{
			return data;
		}
	}

	template<TrivialCopyable T>
	constexpr T FromLittleEndian(T data)
	{
		if constexpr (std::endian::native == std::endian::big)
		{
			return ByteSwap(data);
		}
		else
		{
			return data;
		}
	}

	template<TrivialCopyable T>
	constexpr T ToBigEndian(T data)
	{
		if constexpr (std::endian::native == std::endian::little)
		{
			return ByteSwap(data);
		}
		else
		{
			return data;
		}
	}

	template<TrivialCopyable T>
	constexpr T FromBigEndian(T data)
	{
		if constexpr (std::endian::native == std::endian::little)
		{
			return ByteSwap(data);
		}
		else
		{
			return data;
		}
	}

	TCPStream::TCPStream(NetEndpoint remote, NetEndpoint local) : std::iostream(nullptr), buffer(std::make_unique<TCPStreamBuffer>(std::move(remote), std::move(local)))
	{
		rdbuf(buffer.get());
	}

	TCPStream::TCPStream(std::unique_ptr<TCPStreamBuffer>&& buf) : std::iostream(buf.get()), buffer(std::move(buf)) {}

	TCPStreamStatus TCPStream::connect(NetEndpoint remote, int64_t timeout_ms)
	{
		if (buffer.get() == nullptr) return TCPStreamStatus::Error;
		return buffer->connect(remote, timeout_ms);
	}

	bool TCPStream::is_open()
	{
		if (buffer.get() == nullptr) return false;
		return buffer->status() == TCPStreamStatus::Connected;
	}

	TCPStreamStatus TCPStream::status()
	{
		if (buffer.get() == nullptr) return TCPStreamStatus::Error;
		return buffer->status();
	}

	int64_t TCPStream::recv(std::span<uint8_t> buffer, int64_t timeout_ms)
	{
		if (this->buffer.get() == nullptr) return -1;
		return this->buffer->recv(buffer, timeout_ms);
	}
	int64_t TCPStream::recv(uint8_t* buffer, size_t size, int64_t timeout_ms)
	{
		if (this->buffer.get() == nullptr) return -1;
		return this->buffer->recv(buffer, size, timeout_ms);
	}
	int64_t TCPStream::send(std::span<const uint8_t> buffer, int64_t timeout_ms)
	{
		if (this->buffer.get() == nullptr) return -1;
		return this->buffer->send(buffer, timeout_ms);
	}
	int64_t TCPStream::send(const uint8_t* buffer, size_t size, int64_t timeout_ms)
	{
		if (this->buffer.get() == nullptr) return -1;
		return this->buffer->send(buffer, size, timeout_ms);
	}



	void TCPConnectionProcessor::update()
	{
		if (stream->status() != TCPStreamStatus::Connected) return;

		if (sendBuffer.dataRef.empty())
		{
			auto tmp = getSendData(remote());
			if (tmp != std::nullopt)
			{
				sendBuffer = std::move(*tmp);
			}
		}

		if (!sendBuffer.dataRef.empty())
		{
			if (sendHeadCnt < HeadSize)
			{
				head_t head = sendBuffer.dataRef.size();
				head = ToLittleEndian(head);
				std::span<const uint8_t> sendTmp((const uint8_t*)&head + sendHeadCnt, HeadSize - sendHeadCnt);
				int64_t tmpCnt = stream->send(sendTmp, 0);
				if (tmpCnt <= 0 && stream->status() == TCPStreamStatus::Error)
				{
					if (sendBuffer.status)
					{
						sendBuffer.status->store(AsyncSendStatus::Failed);
						sendBuffer.status.reset();
					}
					sendBuffer.data.clear();
					sendBuffer.dataRef = {};
					sendCnt = 0;
					sendHeadCnt = 0;
					streamError(remote());
					recvBuffer.clear();
					recvCnt = 0;
					headBuffer = {};
					recvHeadCnt = 0;
					return;
				}
				sendHeadCnt += std::max<int64_t>(0, tmpCnt);
			}

			if (sendHeadCnt >= HeadSize)
			{
				std::span<const uint8_t> sendTmp(sendBuffer.dataRef.begin() + sendCnt, sendBuffer.dataRef.end());
				int64_t tmpCnt = stream->send(sendTmp, 0);
				if (tmpCnt <= 0 && stream->status() == TCPStreamStatus::Error)
				{
					if (sendBuffer.status)
					{
						sendBuffer.status->store(AsyncSendStatus::Failed);
						sendBuffer.status.reset();
					}
					sendBuffer.data.clear();
					sendBuffer.dataRef = {};
					sendCnt = 0;
					sendHeadCnt = 0;
					streamError(remote());
					recvBuffer.clear();
					recvCnt = 0;
					headBuffer = {};
					recvHeadCnt = 0;
					return;
				}
				sendCnt += std::max<int64_t>(0, tmpCnt);

				if (sendCnt >= sendBuffer.dataRef.size())
				{
					sendBuffer.data.clear();
					sendBuffer.dataRef = {};
					sendBuffer.status->store(AsyncSendStatus::Sent);
					sendBuffer.status.reset();
					sendCnt = 0;
					sendHeadCnt = 0;
				}
			}
		}

		if (recvCnt < HeadSize)
		{
			std::span<uint8_t> recvTmp{ (uint8_t*)&headBuffer + recvHeadCnt, HeadSize - recvHeadCnt };
			int64_t tmpCnt = stream->recv(recvTmp, 0);
			if (tmpCnt <= 0 && stream->status() == TCPStreamStatus::Error)
			{
				if (sendBuffer.status)
				{
					sendBuffer.status->store(AsyncSendStatus::Failed);
					sendBuffer.status.reset();
				}
				sendBuffer.data.clear();
				sendBuffer.dataRef = {};
				sendCnt = 0;
				sendHeadCnt = 0;
				streamError(remote());
				recvBuffer.clear();
				recvCnt = 0;
				headBuffer = {};
				recvHeadCnt = 0;
				return;
			}
			recvHeadCnt += std::max<int64_t>(0, tmpCnt);

			if (recvHeadCnt >= HeadSize)
			{
				headBuffer = FromLittleEndian(headBuffer);
				recvBuffer.clear();
				recvBuffer.resize(headBuffer);
			}
		}

		if (recvHeadCnt >= HeadSize)
		{
			std::span<uint8_t> recvTmp{ recvBuffer.begin() + recvCnt, recvBuffer.end() };
			int64_t tmpCnt = stream->recv(recvTmp, 0);
			if (tmpCnt <= 0 && stream->status() == TCPStreamStatus::Error)
			{
				if (sendBuffer.status)
				{
					sendBuffer.status->store(AsyncSendStatus::Failed);
					sendBuffer.status.reset();
				}
				sendBuffer.data.clear();
				sendBuffer.dataRef = {};
				sendCnt = 0;
				sendHeadCnt = 0;
				streamError(remote());
				recvBuffer.clear();
				recvCnt = 0;
				headBuffer = {};
				recvHeadCnt = 0;
				return;
			}
			recvCnt += std::max<int64_t>(0, tmpCnt);

			if (recvCnt >= recvBuffer.size())
			{
				pullRecvData(std::move(recvBuffer), remote());
				recvHeadCnt = 0;
				recvCnt = 0;
				headBuffer = {};
			}
		}
	}



	void TCPClient::update()
	{
		processor.update();
	}

	AsyncSendResult TCPClient::asyncSend(std::vector<uint8_t> data)
	{
		if (processor.status() == TCPStreamStatus::Error) return AsyncSendResult{ std::make_shared<std::atomic<AsyncSendStatus>>(AsyncSendStatus::Failed) };
		AsyncSendResult r{ std::make_shared<std::atomic<AsyncSendStatus>>(AsyncSendStatus::InQueue) };
		std::lock_guard lock{ *sendMutex };
		auto dataRef = std::span<const uint8_t>(data);
		sendQueue.emplace_back(std::move(data), dataRef, r.status_);
		return r;
	}

	AsyncSendResult TCPClient::asyncSendRef(std::span<const uint8_t> data)
	{
		if (processor.status() == TCPStreamStatus::Error) return AsyncSendResult{ std::make_shared<std::atomic<AsyncSendStatus>>(AsyncSendStatus::Failed) };
		AsyncSendResult r{ std::make_shared<std::atomic<AsyncSendStatus>>(AsyncSendStatus::InQueue) };
		std::lock_guard lock{ *sendMutex };
		sendQueue.emplace_back(std::vector<uint8_t>{}, data, r.status_);
		return r;
	}

	std::optional<std::vector<uint8_t>> TCPClient::getPackage()
	{
		std::lock_guard lock{ *recvMutex };
		if (!recvQueue.empty())
		{
			auto result = std::make_optional<std::vector<uint8_t>>(std::move(recvQueue.front()));
			recvQueue.pop_front();
			return result;
		}
		else
		{
			return std::nullopt;
		}
	}

	TCPClient::~TCPClient()
	{
		std::lock_guard lock{ *sendMutex };
		for (auto& item : sendQueue)
		{
			item.status->store(AsyncSendStatus::Failed);
		}
	}



	void TCPServer::update()
	{
		{
			std::scoped_lock lock{ *errorMutex, *clientsMutex };
			for (auto& endpoint : errorClients)
			{
				clients.erase(endpoint);
			}
			errorClients.clear();
		}
		auto connection = listener.accept(0);
		if (connection)
		{
			auto remote = connection->remote();
			std::lock_guard lock{ *clientsMutex };
			clients.emplace(remote, Client{ TCPConnectionProcessor
			{
				std::move(connection),
				[this](NetEndpoint remote) -> std::optional<SendQueueItem>
				{
					auto it = clients.find(remote);
					if (it == clients.end()) return std::nullopt;
					auto& client = it->second;
					if (client.sendQueue.empty()) return std::nullopt;
					auto tmp = std::move(client.sendQueue.front());
					client.sendQueue.pop_front();
					return std::make_optional<SendQueueItem>(std::move(tmp));
				},
				[this](std::vector<uint8_t>&& data, NetEndpoint remote)
				{
					std::lock_guard lock{ *recvMutex };
					recvQueue.emplace_back(NetPackage{ std::move(remote), std::move(data) });
				},
				[this](NetEndpoint remote)
				{
					{
						std::lock_guard lock{ *errorMutex };
						errorClients.push_back(remote);
					}
					auto it = clients.find(remote);
					if (it != clients.end())
					{
						auto& client = it->second;
						for (auto& item : client.sendQueue)
						{
							item.status->store(AsyncSendStatus::Failed);
						}
						client.sendQueue.clear();
					}
					std::lock_guard lock{ *sendMutex };
					std::erase_if(sendQueue, [&](auto& item)
						{
						if (item.remote == remote) {
							item.data.status->store(AsyncSendStatus::Failed);
							return true;
						}
						return false;
					});
				}
			}, {} });
		}

		decltype(sendQueue) tmpQueue;
		{
			std::lock_guard lock{ *sendMutex };
			sendQueue.swap(tmpQueue);
		}
		while (!tmpQueue.empty())
		{
			auto& item = tmpQueue.front();
			auto it = clients.find(item.remote);
			if (it != clients.end())
			{
				it->second.sendQueue.push_back(std::move(item.data));
			}
			else
			{
				item.data.status->store(AsyncSendStatus::Failed);
			}
			tmpQueue.pop_front();
		}

		for (auto& client : clients)
		{
			client.second.connection.update();
		}
	}

	std::vector<std::function<void()>> TCPServer::updateTasks()
	{
		{
			std::scoped_lock lock{ *errorMutex, *clientsMutex };
			for (auto& endpoint : errorClients)
			{
				clients.erase(endpoint);
			}
			errorClients.clear();
		}
		auto connection = listener.accept(0);
		if (connection)
		{
			auto remote = connection->remote();
			std::lock_guard lock{ *clientsMutex };
			clients.emplace(remote, Client{ TCPConnectionProcessor
			{
				std::move(connection),
				[this](NetEndpoint remote) -> std::optional<SendQueueItem>
				{
					auto it = clients.find(remote);
					if (it == clients.end()) return std::nullopt;
					auto& client = it->second;
					if (client.sendQueue.empty()) return std::nullopt;
					auto tmp = std::move(client.sendQueue.front());
					client.sendQueue.pop_front();
					return std::make_optional(std::move(tmp));
				},
				[this](std::vector<uint8_t>&& data, NetEndpoint remote)
				{
					std::lock_guard lock{ *recvMutex };
					recvQueue.emplace_back(NetPackage{ std::move(remote), std::move(data) });
				},
				[this](NetEndpoint remote)
				{
					{
						std::lock_guard lock{ *errorMutex };
						errorClients.push_back(remote);
					}
					auto it = clients.find(remote);
					if (it != clients.end())
					{
						auto& client = it->second;
						for (auto& item : client.sendQueue)
						{
							item.status->store(AsyncSendStatus::Failed);
						}
						client.sendQueue.clear();
					}
					std::lock_guard lock{ *sendMutex };
					std::erase_if(sendQueue, [&](auto& item)
						{
						if (item.remote == remote) {
							item.data.status->store(AsyncSendStatus::Failed);
							return true;
						}
						return false;
					});
				}
			}, {} });
		}

		decltype(sendQueue) tmpQueue;
		{
			std::lock_guard lock{ *sendMutex };
			sendQueue.swap(tmpQueue);
		}
		while (!tmpQueue.empty())
		{
			auto& item = tmpQueue.front();
			auto it = clients.find(item.remote);
			if (it != clients.end())
			{
				it->second.sendQueue.push_back(std::move(item.data));
			}
			else
			{
				item.data.status->store(AsyncSendStatus::Failed);
			}
			tmpQueue.pop_front();
		}

		std::vector<std::function<void()>> tasks;
		for (auto& client : clients)
		{
			tasks.push_back([&client]()
			{
				client.second.connection.update();
			});
		}
		return tasks;
	}

	AsyncSendResult TCPServer::asyncSend(std::vector<uint8_t> data, NetEndpoint remote)
	{
		AsyncSendResult r{ std::make_shared<std::atomic<AsyncSendStatus>>(AsyncSendStatus::InQueue) };
		std::lock_guard lock{ *sendMutex };
		auto dataRef = std::span<const uint8_t>(data);
		sendQueue.emplace_back(SendItem{ SendQueueItem{std::move(data), dataRef, r.status_}, remote });
		return r;
	}

	AsyncSendResult TCPServer::asyncSendRef(std::span<const uint8_t> data, NetEndpoint remote)
	{
		AsyncSendResult r{ std::make_shared<std::atomic<AsyncSendStatus>>(AsyncSendStatus::InQueue) };
		std::lock_guard lock{ *sendMutex };
		sendQueue.emplace_back(SendItem{ SendQueueItem{std::vector<uint8_t>{}, data, r.status_}, remote });
		return r;
	}

	std::optional<NetPackage> TCPServer::getPackage()
	{
		std::lock_guard lock{ *recvMutex };
		if (recvQueue.empty()) return std::nullopt;
		auto pkg = std::move(recvQueue.front());
		recvQueue.pop_front();
		return pkg;
	}

	std::vector<NetEndpoint> TCPServer::remote() const
	{
		std::vector<NetEndpoint> remotes;
		std::lock_guard lock{ *clientsMutex };
		for (const auto& client : clients)
		{
			remotes.push_back(client.first);
		}
		return remotes;
	}

	std::optional<TCPStreamStatus> TCPServer::remoteStatus(NetEndpoint remote)
	{
		std::lock_guard lock{ *clientsMutex };
		auto it = clients.find(remote);
		if (it == clients.end()) return std::nullopt;
		return it->second.connection.status();
	}

	TCPServer::~TCPServer()
	{
		for (auto& client : clients)
		{
			for (auto& item : client.second.sendQueue)
			{
				item.status->store(AsyncSendStatus::Failed);
			}
			client.second.sendQueue.clear();
		}
		std::lock_guard lock{ *sendMutex };
		for (auto& item : sendQueue)
		{
			item.data.status->store(AsyncSendStatus::Failed);
		}
	}
}

#ifdef _WIN32
//Win32

namespace tideecho
{
#pragma region Initialize
	bool Initialize()
	{
		WSADATA wsaData;
		return WSAStartup(MAKEWORD(2, 2), &wsaData) == 0;
	}

	void Cleanup()
	{
		WSACleanup();
	}

	NetServiceGuard::NetServiceGuard() : initialized(Initialize()) {}
	NetServiceGuard::~NetServiceGuard()
	{
		if (initialized)
			Cleanup();
	}
	bool NetServiceGuard::valid() const
	{
		return initialized;
	}
	NetServiceGuard::operator bool() const
	{
		return valid();
	}
#pragma endregion


#pragma region Utils
	std::vector<std::pair<AddressFamily, std::string>> GetLocalIPs()
	{
		std::vector<std::pair<AddressFamily, std::string>> ips;
		ULONG size = sizeof(IP_ADAPTER_ADDRESSES);
		PIP_ADAPTER_ADDRESSES result = (PIP_ADAPTER_ADDRESSES)malloc(size);
		if (result == nullptr)
			return ips;

		for (auto flag = GetAdaptersAddresses(AF_UNSPEC, 0, nullptr, result, &size); flag != NO_ERROR;)
		{
			if (flag != ERROR_BUFFER_OVERFLOW)
			{
				free(result);
				return ips;
			}
			free(result);
			result = (PIP_ADAPTER_ADDRESSES)malloc(size);
			if (result == nullptr)
				return ips;

			flag = GetAdaptersAddresses(AF_UNSPEC, 0, nullptr, result, &size);
		}


		auto adapter = result;
		while (adapter)
		{
			auto unicast = adapter->FirstUnicastAddress;
			while (unicast)
			{
				char ip[INET6_ADDRSTRLEN] = { 0 };
				if (unicast->Address.lpSockaddr->sa_family == AF_INET)
				{
					sockaddr_in* sa_in = (sockaddr_in*)unicast->Address.lpSockaddr;
					inet_ntop(AF_INET, &(sa_in->sin_addr), ip, INET_ADDRSTRLEN);
				}
				else if (unicast->Address.lpSockaddr->sa_family == AF_INET6)
				{
					sockaddr_in6* sa_in6 = (sockaddr_in6*)unicast->Address.lpSockaddr;
					inet_ntop(AF_INET6, &(sa_in6->sin6_addr), ip, INET6_ADDRSTRLEN);
				}
				if (ip[0] != '\0')
					ips.emplace_back(unicast->Address.lpSockaddr->sa_family == AF_INET ? AddressFamily::IPv4 : AddressFamily::IPv6, ip);
				unicast = unicast->Next;
			}
			adapter = adapter->Next;
		}
		free(result);
		return ips;
	}

#pragma endregion


#pragma region NetEndpoint

	NetEndpoint::NetEndpoint(const std::string& ip, uint16_t port, AddressFamily family)
	{
		switch (family)
		{
		case AddressFamily::IPv4:
		{
			sockaddr_in tmp{};
			if (inet_pton(AF_INET, ip.c_str(), &tmp.sin_addr) == 1)
			{
				char buf[INET_ADDRSTRLEN] = {};
				inet_ntop(AF_INET, &tmp.sin_addr, buf, INET_ADDRSTRLEN);
				ip_ = buf;
				port_ = port;
				family_ = family;
			}
			return;
		}
		case AddressFamily::IPv6:
		{
			sockaddr_in6 tmp{};
			if (inet_pton(AF_INET6, ip.c_str(), &tmp.sin6_addr) == 1)
			{
				char buf[INET6_ADDRSTRLEN] = {};
				inet_ntop(AF_INET6, &tmp.sin6_addr, buf, INET6_ADDRSTRLEN);
				ip_ = buf;
				port_ = port;
				family_ = family;
			}
			return;
		}
		default:
			return;
		}
	}

	NetEndpoint::NetEndpoint(const std::string& ip_port)
	{
		auto pos = ip_port.rfind(':');
		if (pos == std::string::npos) return;
		std::string ipPart = ip_port.substr(0, pos);
		std::string portPart = ip_port.substr(pos + 1);
		uint64_t port_raw = 0;
		try
		{
			port_raw = std::stoul(portPart);
		}
		catch (...)
		{
			return;
		}
		if (port_raw > 65535)
		{
			return;
		}
		uint16_t port = static_cast<uint16_t>(port_raw);
		if (ipPart.find(':') != std::string::npos)
		{
			sockaddr_in6 tmp{};
			if (inet_pton(AF_INET6, ipPart.c_str(), &tmp.sin6_addr) == 1)
			{
				char buf[INET6_ADDRSTRLEN] = {};
				inet_ntop(AF_INET6, &tmp.sin6_addr, buf, INET6_ADDRSTRLEN);
				ip_ = buf;
				port_ = port;
				family_ = AddressFamily::IPv6;
			}
		}
		else
		{
			sockaddr_in tmp{};
			if (inet_pton(AF_INET, ipPart.c_str(), &tmp.sin_addr) == 1)
			{
				char buf[INET_ADDRSTRLEN] = {};
				inet_ntop(AF_INET, &tmp.sin_addr, buf, INET_ADDRSTRLEN);
				ip_ = buf;
				port_ = port;
				family_ = AddressFamily::IPv4;
			}
		}
	}

#pragma endregion


#pragma region Socket

	void Socket::reset() noexcept
	{
		if (s == invalid_socket) return;
		SOCKET native = I64ToSocket(s);
		closesocket(native);
		s = invalid_socket;
	}
	
#pragma endregion


#pragma region TCPStreamBuffer
	TCPStreamBuffer::TCPStreamBuffer(NetEndpoint remote, NetEndpoint local)
	{
		setp(reinterpret_cast<char*>(writeBuf.data()), reinterpret_cast<char*>(writeBuf.data()) + writeBuf.size());
		if (!remote.valid() && !local.valid())
		{
			status_ = TCPStreamStatus::Error;
			return;
		}
		SOCKET sock;
		if (local.valid())
		{
			sock = socket(local.addrFamily() == AddressFamily::IPv4 ? AF_INET : AF_INET6, SOCK_STREAM, IPPROTO_TCP);
			if (sock == INVALID_SOCKET)
			{
				status_ = TCPStreamStatus::Error;
				return;
			}
		}
		else
		{
			sock = socket(remote.addrFamily() == AddressFamily::IPv4 ? AF_INET : AF_INET6, SOCK_STREAM, IPPROTO_TCP);
			if (sock == INVALID_SOCKET)
			{
				status_ = TCPStreamStatus::Error;
				return;
			}
			family = remote.addrFamily();
		}

		u_long mode = 1;
		if (ioctlsocket(sock, FIONBIO, &mode) == SOCKET_ERROR)
		{
			closesocket(sock);
			status_ = TCPStreamStatus::Error;
			return;
		}

		if (local.valid())
		{
			if (local.addrFamily() == AddressFamily::IPv4)
			{
				auto addrTmp = EndpointToSockaddr(local);//Endpoint保证有效性，所以一定转换成功
				sockaddr_in addr = std::get<sockaddr_in>(*addrTmp);
				if (bind(sock, (sockaddr*)&addr, sizeof(sockaddr_in)) == SOCKET_ERROR)
				{
					local_ = {};
					if (!remote.valid())
					{
						closesocket(sock);
						status_ = TCPStreamStatus::Error;
						return;
					}
					else if (remote.addrFamily() == AddressFamily::IPv6)
					{
						closesocket(sock);
						sock = socket(AF_INET6, SOCK_STREAM, IPPROTO_TCP);
						if (sock == INVALID_SOCKET)
						{
							status_ = TCPStreamStatus::Error;
							return;
						}

						u_long mode = 1;
						if (ioctlsocket(sock, FIONBIO, &mode) == SOCKET_ERROR)
						{
							closesocket(sock);
							status_ = TCPStreamStatus::Error;
							return;
						}
						family = AddressFamily::IPv6;
					}
					else
					{
						family = AddressFamily::IPv4;
					}
				}
				else
				{
					family = AddressFamily::IPv4;
				}
			}
			else
			{
				auto addrTmp = EndpointToSockaddr(local);
				sockaddr_in6 addr = std::get<sockaddr_in6>(*addrTmp);
				if (bind(sock, (sockaddr*)&addr, sizeof(sockaddr_in6)) == SOCKET_ERROR)
				{
					local_ = {};
					if (!remote.valid())
					{
						closesocket(sock);
						status_ = TCPStreamStatus::Error;
						return;
					}
					else if (remote.addrFamily() == AddressFamily::IPv4)
					{
						closesocket(sock);
						sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
						if (sock == INVALID_SOCKET)
						{
							status_ = TCPStreamStatus::Error;
							return;
						}

						u_long mode = 1;
						if (ioctlsocket(sock, FIONBIO, &mode) == SOCKET_ERROR)
						{
							closesocket(sock);
							status_ = TCPStreamStatus::Error;
							return;
						}
						family = AddressFamily::IPv4;
					}
					else
					{
						family = AddressFamily::IPv6;
					}
				}
				else
				{
					family = AddressFamily::IPv6;
				}
			}
		}

		int keepalive_enabled = 1;
		if (setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (char*)&keepalive_enabled, sizeof(keepalive_enabled)) == SOCKET_ERROR) {
			status_ = TCPStreamStatus::Error;
			closesocket(sock);
			return;
		}
		struct tcp_keepalive ka = { 0 };
		ka.onoff = 1;
		ka.keepalivetime = 20000; // 空闲超时(ms)
		ka.keepaliveinterval = 5000; // 探测间隔(ms)
		DWORD ret = 0;
		if (WSAIoctl(sock, SIO_KEEPALIVE_VALS, &ka, sizeof(ka), NULL, 0, &ret, NULL, NULL) == SOCKET_ERROR) {
			status_ = TCPStreamStatus::Error;
			closesocket(sock);
			return;
		}

		s = Socket(SocketToI64(sock));

		if (remote.valid() && (!local_.valid() || remote.addrFamily() == local.addrFamily()))
		{
			connect(remote, 0);
		}
		else
		{
			remote_ = {};
		}

		sockaddr_storage lAddr = {};
		socklen_t len = sizeof(sockaddr_storage);
		if (getsockname(sock, (sockaddr*)(&lAddr), &len) == SOCKET_ERROR)
		{
			local_ = {};
		}
		else
		{
			if (lAddr.ss_family == AF_INET)
			{
				sockaddr_in* pAddr = (sockaddr_in*)&lAddr;
				char ip[INET_ADDRSTRLEN] = {};
				if (inet_ntop(AF_INET, &(pAddr->sin_addr), ip, INET_ADDRSTRLEN) == nullptr)
				{
					local_ = {};
				}
				else
				{
					local_ = NetEndpoint{ ip, ntohs(pAddr->sin_port), AddressFamily::IPv4 };
				}
			}
			else if (lAddr.ss_family == AF_INET6)
			{
				sockaddr_in6* pAddr = (sockaddr_in6*)&lAddr;
				char ip[INET6_ADDRSTRLEN] = {};
				if (inet_ntop(AF_INET6, &(pAddr->sin6_addr), ip, INET6_ADDRSTRLEN) == nullptr)
				{
					local_ = {};
				}
				else
				{
					local_ = NetEndpoint{ ip, ntohs(pAddr->sin6_port), AddressFamily::IPv6 };
				}
			}
			else
			{
				local_ = {};
			}
		}
	}

	TCPStreamBuffer::TCPStreamBuffer(TCPStreamBuffer&& other) noexcept :
		std::streambuf(std::move(other)),
		s(std::move(other.s)),
		readBuf(std::move(other.readBuf)),
		writeBuf(std::move(other.writeBuf)),
		remote_(std::move(other.remote_)),
		local_(std::move(other.local_)),
		status_(other.status_),
		family(other.family)
	{
		other.status_ = TCPStreamStatus::Error;
		other.family = AddressFamily::Unknown;
	}

	TCPStreamBuffer::TCPStreamBuffer(Socket&& sock) : s(std::move(sock))
	{
		setp(reinterpret_cast<char*>(writeBuf.data()), reinterpret_cast<char*>(writeBuf.data()) + writeBuf.size());

		if (!s.valid()) {
			status_ = TCPStreamStatus::Error;
			return;
		}

		SOCKET native = I64ToSocket(s.get());
	
		// 设为非阻塞（与主动连接保持一致）
		u_long mode = 1;
		ioctlsocket(native, FIONBIO, &mode);

		// 获取本地地址，并推断地址族
		struct sockaddr_storage ss_local;
		socklen_t len = sizeof(ss_local);
		if (getsockname(native, (sockaddr*)&ss_local, &len) == 0)
		{
			if (ss_local.ss_family == AF_INET)
			{
				family = AddressFamily::IPv4;
				auto* addr = (sockaddr_in*)&ss_local;
				char ip[INET_ADDRSTRLEN];
				inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip));
				local_ = NetEndpoint(ip, ntohs(addr->sin_port), AddressFamily::IPv4);
			}
			else if (ss_local.ss_family == AF_INET6)
			{
				family = AddressFamily::IPv6;
				auto* addr = (sockaddr_in6*)&ss_local;
				char ip[INET6_ADDRSTRLEN];
				inet_ntop(AF_INET6, &addr->sin6_addr, ip, sizeof(ip));
				local_ = NetEndpoint(ip, ntohs(addr->sin6_port), AddressFamily::IPv6);
			}
		}

		// 获取远端地址
		struct sockaddr_storage ss_remote;
		len = sizeof(ss_remote);
		if (getpeername(native, (sockaddr*)&ss_remote, &len) == 0)
		{
			if (ss_remote.ss_family == AF_INET)
			{
				auto* addr = (sockaddr_in*)&ss_remote;
				char ip[INET_ADDRSTRLEN];
				inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip));
				remote_ = NetEndpoint(ip, ntohs(addr->sin_port), AddressFamily::IPv4);
			} 
			else if (ss_remote.ss_family == AF_INET6)
			{
				auto* addr = (sockaddr_in6*)&ss_remote;
				char ip[INET6_ADDRSTRLEN];
				inet_ntop(AF_INET6, &addr->sin6_addr, ip, sizeof(ip));
				remote_ = NetEndpoint(ip, ntohs(addr->sin6_port), AddressFamily::IPv6);
			}
		}
		status();
	}

	TCPStreamBuffer& TCPStreamBuffer::operator=(TCPStreamBuffer&& other) noexcept
	{
		if (this != &other)
		{
			reset();
			std::streambuf::operator=(std::move(other));
			s = std::move(other.s);
			readBuf = std::move(other.readBuf);
			writeBuf = std::move(other.writeBuf);
			remote_ = std::move(other.remote_);
			local_ = std::move(other.local_);
			status_ = other.status_;
			family = other.family;
			other.status_ = TCPStreamStatus::Error;
			other.family = AddressFamily::Unknown;
		}
		return *this;
	}

	void TCPStreamBuffer::reset()
	{
		s.reset();
		remote_ = {};
		local_ = {};
		status_ = TCPStreamStatus::Error;
		family = AddressFamily::Unknown;
	}

	TCPStreamStatus TCPStreamBuffer::connect(NetEndpoint remote, int64_t timeout_ms)
	{
		// 0. 状态检查，并更新状态
		status();
		if (status_ != TCPStreamStatus::Idle)
		{
			return TCPStreamStatus::Error;
		}

		// 1. 检查远端地址族是否匹配当前 socket 的地址族
		if (remote.addrFamily() != family)
		{
			return TCPStreamStatus::Error;
		}

		if (!s.valid())
		{
			reset();
			return TCPStreamStatus::Error;
		}

		SOCKET sock = I64ToSocket(s.get());
		

		// 2. 转换远端地址
		auto addrOpt = EndpointToSockaddr(remote);
		if (!addrOpt)
		{
			return TCPStreamStatus::Error;
		}

		sockaddr* addrPtr = nullptr;
		int addrLen = 0;
		if (family == AddressFamily::IPv4)
		{
			auto& addr = std::get<sockaddr_in>(*addrOpt);
			addrPtr = reinterpret_cast<sockaddr*>(&addr);
			addrLen = sizeof(addr);
		}
		else if (family == AddressFamily::IPv6)
		{
			auto& addr = std::get<sockaddr_in6>(*addrOpt);
			addrPtr = reinterpret_cast<sockaddr*>(&addr);
			addrLen = sizeof(addr);
		}

		// 3. 处理 timeout < 0：临时阻塞模式
		if (timeout_ms < 0)
		{
			// 临时设为阻塞模式（构造函数已确保非阻塞）
			u_long blocking = 0;
			if (ioctlsocket(sock, FIONBIO, &blocking) == SOCKET_ERROR)
			{
				reset();
				return TCPStreamStatus::Error;
			}
			connectCalled = true;
			int ret = ::connect(sock, addrPtr, addrLen);
			// 恢复非阻塞模式
			u_long nonblock = 1;
			ioctlsocket(sock, FIONBIO, &nonblock);   // 忽略恢复失败

			if (ret == 0)
			{
				remote_ = std::move(remote);
				status_ = TCPStreamStatus::Connected;
				return TCPStreamStatus::Connected;
			}
			else
			{
				reset();
				return TCPStreamStatus::Error;
			}
		}

		// 4. 非阻塞连接（timeout >= 0）
		connectCalled = true;
		int ret = ::connect(sock, addrPtr, addrLen);
		if (ret == 0)
		{
			remote_ = std::move(remote);
			status_ = TCPStreamStatus::Connected;
			return TCPStreamStatus::Connected;
		}
		if (WSAGetLastError() != WSAEWOULDBLOCK)
		{
			reset();
			return TCPStreamStatus::Error;
		}

		// 连接正在建立
		if (timeout_ms == 0)
		{
			remote_ = std::move(remote);
			status_ = TCPStreamStatus::Connecting;
			return TCPStreamStatus::Connecting;
		}

		// 5. timeout_ms > 0：轮询连接状态，间隔 100→200→400→1000→1000... μs
		using namespace std::chrono;
		auto start = steady_clock::now();
		auto timeout_dur = milliseconds(timeout_ms);
		int delay_us = 100;
		int step = 0;   // 0->100, 1->200, 2->400, >=3->1000

		while (true)
		{
			auto now = steady_clock::now();
			auto elapsed = now - start;
			if (elapsed >= timeout_dur)
			{
				remote_ = std::move(remote);
				status_ = TCPStreamStatus::Connecting;
				return TCPStreamStatus::Connecting;
			}

			// 计算本次等待时间（微秒）
			int wait_us = delay_us;
			auto remain_us = duration_cast<microseconds>(timeout_dur - elapsed).count();
			if (wait_us > remain_us)
			{
				wait_us = static_cast<int>(remain_us);
			}

			fd_set writefds;
			FD_ZERO(&writefds);
			FD_SET(sock, &writefds);
			struct timeval tv;
			tv.tv_sec = wait_us / 1000000;
			tv.tv_usec = wait_us % 1000000;

			int sel = select(0, nullptr, &writefds, nullptr, &tv);
			if (sel == SOCKET_ERROR)
			{
				reset();
				return TCPStreamStatus::Error;
			}

			if (sel == 0)
			{
				// 超时，继续下一轮，增加延迟
				if (step == 0) delay_us = 200;
				else if (step == 1) delay_us = 400;
				else delay_us = 1000;
				step++;
				continue;
			}

			// socket 可写，检查连接结果
			int so_error = 0;
			socklen_t len = sizeof(so_error);
			if (getsockopt(sock, SOL_SOCKET, SO_ERROR, (char*)&so_error, &len) == 0)
			{
				if (so_error == 0)
				{
					remote_ = std::move(remote);
					status_ = TCPStreamStatus::Connected;
					return TCPStreamStatus::Connected;
				}
				else
				{
					reset();
					return TCPStreamStatus::Error;
				}
			}
			else
			{
				reset();
				return TCPStreamStatus::Error;
			}
		}
	}

	TCPStreamStatus TCPStreamBuffer::status()
	{
		// 1. 若已为错误状态，直接返回
		if (status_ == TCPStreamStatus::Error)
			return TCPStreamStatus::Error;

		// 2. 获取套接字句柄，校验有效性
		SOCKET sock = I64ToSocket(s.get());
		if (sock == INVALID_SOCKET) {
			status_ = TCPStreamStatus::Error;
			s.reset();
			return status_;
		}

		// 3. 如果从未调用过 connect，直接返回 Idle
		if (!connectCalled) {
			status_ = TCPStreamStatus::Idle;
			return status_;
		}

		// 4. 检查连接过程状态（通过 SO_ERROR）
		int error = 0;
		int optlen = sizeof(error);
		int ret = getsockopt(sock, SOL_SOCKET, SO_ERROR, (char*)&error, &optlen);
		if (ret == SOCKET_ERROR) {
			// getsockopt 失败，保守处理为错误
			status_ = TCPStreamStatus::Error;
			s.reset();
			return status_;
		}

		// 根据错误码判定连接阶段
		if (error == WSAEWOULDBLOCK || error == WSAEINPROGRESS) {
			// 连接仍在进行中（非阻塞 connect 尚未完成）
			status_ = TCPStreamStatus::Connecting;
			return status_;
		}
		else if (error != 0) {
			// 连接失败（如 WSAETIMEDOUT、WSAECONNREFUSED 等）
			status_ = TCPStreamStatus::Error;
			s.reset();
			return status_;
		}

		// 此时 error == 0，表示连接已成功建立（或曾经成功）
		// 5. 检测已建立连接是否仍然存活（被动断开检测）
		fd_set readfds;
		FD_ZERO(&readfds);
		FD_SET(sock, &readfds);
		timeval tv = { 0, 0 };  // 立即返回，不阻塞
		int selRet = select(0, &readfds, nullptr, nullptr, &tv);

		if (selRet == SOCKET_ERROR) {
			status_ = TCPStreamStatus::Error;
			s.reset();
			return status_;
		}

		if (selRet == 0) {
			// 无可读事件，连接正常（无数据，无断开）
			status_ = TCPStreamStatus::Connected;
			return status_;
		}

		// 套接字可读：可能是数据，也可能对端关闭或出错
		// 使用 MSG_PEEK 窥探一个字节，不消耗数据
		char buf[1];
		int recvRet = ::recv(sock, buf, 1, MSG_PEEK);
		if (recvRet == 0) {
			// 对端正常关闭连接 (FIN)
			status_ = TCPStreamStatus::Error;
			s.reset();
			return status_;
		}
		else if (recvRet == SOCKET_ERROR) {
			int err = WSAGetLastError();
			if (err == WSAEWOULDBLOCK) {
				// 理论上 select 已报告可读，不应出现，但若发生则视为连接正常
				status_ = TCPStreamStatus::Connected;
			}
			else {
				// 其他错误（如 WSAECONNRESET、WSAENETRESET 等）
				status_ = TCPStreamStatus::Error;
				s.reset();
			}
			return status_;
		}
		else {
			// 有数据可读，连接正常
			status_ = TCPStreamStatus::Connected;
			return status_;
		}
	}

	int64_t TCPStreamBuffer::send(std::span<const uint8_t> data, int64_t timeout_ms)
	{
		status();//更新状态
		if (data.empty())
		{
			return 0;   // 空数据，成功发送 0 字节
		}

		SOCKET sock = I64ToSocket(s.get());
		if (sock == INVALID_SOCKET)
		{
			status_ = TCPStreamStatus::Error;
			return -1;
		}

		if (status_ != TCPStreamStatus::Connected)
		{
			return -1;
		}

		// 阻塞模式：临时切换
		if (timeout_ms < 0)
		{
			u_long blocking = 0;
			if (ioctlsocket(sock, FIONBIO, &blocking) == SOCKET_ERROR)
			{
				status_ = TCPStreamStatus::Error;
				s.reset();
				return -1;
			}
			int ret = ::send(sock, reinterpret_cast<const char*>(data.data()), static_cast<int>(data.size()), 0);
			u_long nonblock = 1;
			ioctlsocket(sock, FIONBIO, &nonblock);

			if (ret > 0)
				return ret;
			if (ret == 0)   // 对端关闭（send 理论上不会返回 0，但防御）
			{
				status_ = TCPStreamStatus::Error;
				s.reset();
				return 0;
			}
			// ret == -1
			int err = WSAGetLastError();
			if (err == WSAEWOULDBLOCK)  // 阻塞模式下不应出现，但若出现视为临时
				return -1;
			status_ = TCPStreamStatus::Error;
			s.reset();
			return -1;
		}

		// 非阻塞或超时模式
		int ret = ::send(sock, reinterpret_cast<const char*>(data.data()), static_cast<int>(data.size()), 0);
		if (ret > 0)
			return ret;
		if (ret == 0)   // 对端关闭
		{
			status_ = TCPStreamStatus::Error;
			s.reset();
			return 0;
		}
		int err = WSAGetLastError();
		if (err != WSAEWOULDBLOCK)
		{
			status_ = TCPStreamStatus::Error;
			s.reset();
			return -1;
		}

		// 临时不可用 (WSAEWOULDBLOCK)
		if (timeout_ms == 0)
		{
			// 立即返回 -1，但状态不变，调用者可以重试
			return -1;
		}

		// timeout_ms > 0：轮询可写，直到可写或超时
		using namespace std::chrono;
		auto start = steady_clock::now();
		auto timeout_dur = milliseconds(timeout_ms);
		int delay_us = 100;
		int step = 0;

		while (true)
		{
			auto now = steady_clock::now();
			auto elapsed = now - start;
			if (elapsed >= timeout_dur)
			{
				// 超时，未发送任何字节
				return -1;
			}

			int wait_us = delay_us;
			auto remain_us = duration_cast<microseconds>(timeout_dur - elapsed).count();
			if (wait_us > remain_us)
				wait_us = static_cast<int>(remain_us);

			fd_set writefds;
			FD_ZERO(&writefds);
			FD_SET(sock, &writefds);
			struct timeval tv;
			tv.tv_sec = wait_us / 1000000;
			tv.tv_usec = wait_us % 1000000;

			int sel = select(0, nullptr, &writefds, nullptr, &tv);
			if (sel == SOCKET_ERROR)
			{
				status_ = TCPStreamStatus::Error;
				s.reset();
				return -1;
			}
			if (sel == 0)
			{
				if (step == 0) delay_us = 200;
				else if (step == 1) delay_us = 400;
				else delay_us = 1000;
				step++;
				continue;
			}

			// 可写，尝试发送
			ret = ::send(sock, reinterpret_cast<const char*>(data.data()), static_cast<int>(data.size()), 0);
			if (ret > 0)
				return ret;
			if (ret == 0)
			{
				status_ = TCPStreamStatus::Error;
				s.reset();
				return 0;
			}
			err = WSAGetLastError();
			if (err == WSAEWOULDBLOCK)
				continue;   // 极少情况，继续等待
			status_ = TCPStreamStatus::Error;
			s.reset();
			return -1;
		}
	}

	int64_t TCPStreamBuffer::send(const uint8_t* buffer, size_t size, int64_t timeout_ms)
	{
		return send(std::span<const uint8_t>(buffer, size), timeout_ms);
	}

	int64_t TCPStreamBuffer::recv(std::span<uint8_t> buffer, int64_t timeout_ms)
	{
		status();//更新状态
		if (buffer.empty())
		{
			return 0;
		}

		SOCKET sock = I64ToSocket(s.get());
		if (sock == INVALID_SOCKET)
		{
			status_ = TCPStreamStatus::Error;
			return -1;
		}

		if (status_ != TCPStreamStatus::Connected)
		{
			return -1;
		}

		// 阻塞模式
		if (timeout_ms < 0)
		{
			u_long blocking = 0;
			if (ioctlsocket(sock, FIONBIO, &blocking) == SOCKET_ERROR)
			{
				status_ = TCPStreamStatus::Error;
				s.reset();
				return -1;
			}
			int ret = ::recv(sock, reinterpret_cast<char*>(buffer.data()), static_cast<int>(buffer.size()), 0);
			u_long nonblock = 1;
			ioctlsocket(sock, FIONBIO, &nonblock);

			if (ret > 0)
				return ret;
			if (ret == 0)   // 对端关闭
			{
				status_ = TCPStreamStatus::Error;
				s.reset();
				return 0;
			}
			status_ = TCPStreamStatus::Error;
			s.reset();
			return -1;
		}

		// 非阻塞立即
		int ret = ::recv(sock, reinterpret_cast<char*>(buffer.data()), static_cast<int>(buffer.size()), 0);
		if (ret > 0)
			return ret;
		if (ret == 0)   // 对端关闭
		{
			status_ = TCPStreamStatus::Error;
			s.reset();
			return 0;
		}
		int err = WSAGetLastError();
		if (err != WSAEWOULDBLOCK)
		{
			status_ = TCPStreamStatus::Error;
			s.reset();
			return -1;
		}

		// 临时无数据
		if (timeout_ms == 0)
		{
			return -1;   // 无数据且不等待，返回 -1（非致命）
		}

		// timeout_ms > 0：轮询可读
		using namespace std::chrono;
		auto start = steady_clock::now();
		auto timeout_dur = milliseconds(timeout_ms);
		int delay_us = 100;
		int step = 0;

		while (true)
		{
			auto now = steady_clock::now();
			auto elapsed = now - start;
			if (elapsed >= timeout_dur)
			{
				return -1;   // 超时无数据
			}

			int wait_us = delay_us;
			auto remain_us = duration_cast<microseconds>(timeout_dur - elapsed).count();
			if (wait_us > remain_us)
				wait_us = static_cast<int>(remain_us);

			fd_set readfds;
			FD_ZERO(&readfds);
			FD_SET(sock, &readfds);
			struct timeval tv;
			tv.tv_sec = wait_us / 1000000;
			tv.tv_usec = wait_us % 1000000;

			int sel = select(0, &readfds, nullptr, nullptr, &tv);
			if (sel == SOCKET_ERROR)
			{
				status_ = TCPStreamStatus::Error;
				s.reset();
				return -1;
			}
			if (sel == 0)
			{
				if (step == 0) delay_us = 200;
				else if (step == 1) delay_us = 400;
				else delay_us = 1000;
				step++;
				continue;
			}

			ret = ::recv(sock, reinterpret_cast<char*>(buffer.data()), static_cast<int>(buffer.size()), 0);
			if (ret > 0)
				return ret;
			if (ret == 0)   // 对端关闭
			{
				status_ = TCPStreamStatus::Error;
				s.reset();
				return 0;
			}
			err = WSAGetLastError();
			if (err == WSAEWOULDBLOCK)
				continue;
			status_ = TCPStreamStatus::Error;
			s.reset();
			return -1;
		}
	}

	int64_t TCPStreamBuffer::recv(uint8_t* buffer, size_t size, int64_t timeout_ms)
	{
		return recv(std::span<uint8_t>(buffer, size), timeout_ms);
	}

	std::streambuf::int_type TCPStreamBuffer::underflow()
	{
		// 如果缓冲区中还有数据，直接返回
		if (gptr() < egptr())
			return traits_type::to_int_type(*gptr());

		// 阻塞读取数据到 readBuf
		int64_t ret = recv({ readBuf.data(), readBuf.size() }, -1);
		if (ret <= 0)   // 0: 对端关闭, -1: 错误
			return traits_type::eof();

		// 设置新的读区域
		setg(reinterpret_cast<char*>(readBuf.data()), reinterpret_cast<char*>(readBuf.data()), reinterpret_cast<char*>(readBuf.data()) + static_cast<size_t>(ret));
		return traits_type::to_int_type(*gptr());
	}

	std::streambuf::int_type TCPStreamBuffer::overflow(std::streambuf::int_type c)
	{
		// 刷新输出缓冲区（如果有待发送数据）
		if (pbase() != nullptr && pptr() > pbase())
		{
			size_t size = static_cast<size_t>(pptr() - pbase());
			int64_t sent = send({ reinterpret_cast<uint8_t*>(pbase()), size }, -1);
			if (sent != static_cast<int64_t>(size))
				return traits_type::eof();
			pbump(-static_cast<int>(size));   // 重置写指针
		}

		// 处理需要额外写入的字符
		if (!traits_type::eq_int_type(c, traits_type::eof()))
		{
			// 此时写缓冲区已清空，一定有空闲位置
			if (pptr() < epptr())
			{
				*pptr() = traits_type::to_char_type(c);
				pbump(1);
			}
			else
			{
				// 极端情况：缓冲区太小，直接发送单个字符
				uint8_t ch = traits_type::to_char_type(c);
				int64_t sent = send({ &ch, 1 }, -1);
				if (sent != 1)
					return traits_type::eof();
			}
		}
		return traits_type::not_eof(c);
	}

	// 可选：同步缓冲区，刷新所有输出
	int TCPStreamBuffer::sync()
	{
		if (pbase() != nullptr && pptr() > pbase())
		{
			size_t size = static_cast<size_t>(pptr() - pbase());
			int64_t sent = send({ reinterpret_cast<uint8_t*>(pbase()), size }, -1);
			if (sent != static_cast<int64_t>(size))
				return -1;
			pbump(-static_cast<int>(size));
		}
		return 0;
	}
#pragma endregion


#pragma region TCPListener

	TCPListener::TCPListener(const NetEndpoint& local)
	{
		if (!local.valid()) return;
		SOCKET sock = socket(local.addrFamily() == AddressFamily::IPv4 ? AF_INET : AF_INET6, SOCK_STREAM, IPPROTO_TCP);
		if (sock == INVALID_SOCKET) return;

		u_long mode = 1;
		if (ioctlsocket(sock, FIONBIO, &mode) == SOCKET_ERROR)
		{
			closesocket(sock);
			return;
		}

		if (local.addrFamily() == AddressFamily::IPv6)
		{
			int opt = 0;
			if (setsockopt(sock, IPPROTO_IPV6, IPV6_V6ONLY, (char*)&opt, sizeof(opt)) == SOCKET_ERROR)
			{
				closesocket(sock);
				return;
			}
		}

		if (local.addrFamily() == AddressFamily::IPv4)
		{
			auto addr = std::get<sockaddr_in>(*EndpointToSockaddr(local));//有效endpoint必成功转换
			if (bind(sock, (sockaddr*)&addr, sizeof(sockaddr_in)) == SOCKET_ERROR)
			{
				closesocket(sock);
				return;
			}
			if (listen(sock, SOMAXCONN) == SOCKET_ERROR)
			{
				closesocket(sock);
				return;
			}
		}
		else
		{
			auto addr = std::get<sockaddr_in6>(*EndpointToSockaddr(local));//有效endpoint必成功转换
			if (bind(sock, (sockaddr*)&addr, sizeof(sockaddr_in6)) == SOCKET_ERROR)
			{
				closesocket(sock);
				return;
			}
			if (listen(sock, SOMAXCONN) == SOCKET_ERROR)
			{
				closesocket(sock);
				return;
			}
		}

		s = Socket(sock);
		status_ = TCPListenerStatus::Listening;
		local_ = local;

		if (local.addrFamily() == AddressFamily::IPv4)
		{
			sockaddr_in actualAddr;
			socklen_t len = sizeof(actualAddr);
			if (getsockname(sock, (sockaddr*)&actualAddr, &len) == 0)
			{
				char ip[INET_ADDRSTRLEN];
				inet_ntop(AF_INET, &actualAddr.sin_addr, ip, sizeof(ip));
				local_ = NetEndpoint(ip, ntohs(actualAddr.sin_port), AddressFamily::IPv4);
			}
		}
		else
		{
			sockaddr_in6 actualAddr;
			socklen_t len = sizeof(actualAddr);
			if (getsockname(sock, (sockaddr*)&actualAddr, &len) == 0)
			{
				char ip[INET6_ADDRSTRLEN];
				inet_ntop(AF_INET6, &actualAddr.sin6_addr, ip, sizeof(ip));
				local_ = NetEndpoint(ip, ntohs(actualAddr.sin6_port), AddressFamily::IPv6);
			}
		}
	}

	std::unique_ptr<TCPStream> TCPListener::accept(int64_t timeout_ms) {
		if (!valid() || status_ != TCPListenerStatus::Listening) {
			return nullptr;
		}

		SOCKET listenSock = I64ToSocket(s.get());
		if (listenSock == INVALID_SOCKET) {
			return nullptr;
		}

		// 临时阻塞模式
		if (timeout_ms < 0) {
			u_long blocking = 0;
			if (ioctlsocket(listenSock, FIONBIO, &blocking) == SOCKET_ERROR) {
				return nullptr;
			}
			SOCKET client = ::accept(listenSock, nullptr, nullptr);
			u_long nonblock = 1;
			ioctlsocket(listenSock, FIONBIO, &nonblock);
			if (client == INVALID_SOCKET) {
				return nullptr;
			}
			ioctlsocket(client, FIONBIO, &nonblock);

			int keepalive_enabled = 1;
			if (setsockopt(client, SOL_SOCKET, SO_KEEPALIVE, (char*)&keepalive_enabled, sizeof(keepalive_enabled)) == SOCKET_ERROR) {
				closesocket(client);
				return nullptr;
			}
			struct tcp_keepalive ka = { 0 };
			ka.onoff = 1;
			ka.keepalivetime = 20000; // 空闲超时(ms)
			ka.keepaliveinterval = 5000; // 探测间隔(ms)
			DWORD ret = 0;
			if (WSAIoctl(client, SIO_KEEPALIVE_VALS, &ka, sizeof(ka), NULL, 0, &ret, NULL, NULL) == SOCKET_ERROR) {
				closesocket(client);
				return nullptr;
			}

			auto newStreamBuffer = std::make_unique<TCPStreamBuffer>(Socket(SocketToI64(client)));
			newStreamBuffer->connectCalled = true; // 标记为已连接
			return std::make_unique<TCPStream>(std::move(newStreamBuffer));
		}

		// 非阻塞立即尝试
		if (timeout_ms == 0) {
			SOCKET client = ::accept(listenSock, nullptr, nullptr);
			if (client != INVALID_SOCKET) {
				u_long nonblock = 1;
				ioctlsocket(client, FIONBIO, &nonblock);

				int keepalive_enabled = 1;
				if (setsockopt(client, SOL_SOCKET, SO_KEEPALIVE, (char*)&keepalive_enabled, sizeof(keepalive_enabled)) == SOCKET_ERROR) {
					closesocket(client);
					return nullptr;
				}
				struct tcp_keepalive ka = { 0 };
				ka.onoff = 1;
				ka.keepalivetime = 20000; // 空闲超时(ms)
				ka.keepaliveinterval = 5000; // 探测间隔(ms)
				DWORD ret = 0;
				if (WSAIoctl(client, SIO_KEEPALIVE_VALS, &ka, sizeof(ka), NULL, 0, &ret, NULL, NULL) == SOCKET_ERROR) {
					closesocket(client);
					return nullptr;
				}

				auto newStreamBuffer = std::make_unique<TCPStreamBuffer>(Socket(SocketToI64(client)));
				newStreamBuffer->connectCalled = true; // 标记为已连接
				return std::make_unique<TCPStream>(std::move(newStreamBuffer));
			}
			int err = WSAGetLastError();
			if (err == WSAEWOULDBLOCK) {
				return nullptr;  // 无连接，非致命
			}
			return nullptr;
		}

		// timeout_ms > 0：轮询 select，递增延迟
		using namespace std::chrono;
		auto start = steady_clock::now();
		auto timeout_dur = milliseconds(timeout_ms);
		int delay_us = 100;
		int step = 0;

		while (true) {
			auto now = steady_clock::now();
			auto elapsed = now - start;
			if (elapsed >= timeout_dur) {
				return nullptr;
			}

			int wait_us = delay_us;
			auto remain_us = duration_cast<microseconds>(timeout_dur - elapsed).count();
			if (wait_us > remain_us) {
				wait_us = static_cast<int>(remain_us);
			}

			fd_set readfds;
			FD_ZERO(&readfds);
			FD_SET(listenSock, &readfds);
			struct timeval tv;
			tv.tv_sec = wait_us / 1000000;
			tv.tv_usec = wait_us % 1000000;

			int sel = select(0, &readfds, nullptr, nullptr, &tv);
			if (sel == SOCKET_ERROR) {
				return nullptr;
			}
			if (sel == 0) {
				// 未就绪，增加延迟
				if (step == 0) delay_us = 200;
				else if (step == 1) delay_us = 400;
				else delay_us = 1000;
				step++;
				continue;
			}

			// 有连接到来
			SOCKET client = ::accept(listenSock, nullptr, nullptr);
			if (client == INVALID_SOCKET) {
				int err = WSAGetLastError();
				if (err == WSAEWOULDBLOCK) {
					continue;
				}
				return nullptr;
			}
			u_long nonblock = 1;
			ioctlsocket(client, FIONBIO, &nonblock);

			int keepalive_enabled = 1;
			if (setsockopt(client, SOL_SOCKET, SO_KEEPALIVE, (char*)&keepalive_enabled, sizeof(keepalive_enabled)) == SOCKET_ERROR) {
				closesocket(client);
				return nullptr;
			}
			struct tcp_keepalive ka = { 0 };
			ka.onoff = 1;
			ka.keepalivetime = 20000; // 空闲超时(ms)
			ka.keepaliveinterval = 5000; // 探测间隔(ms)
			DWORD ret = 0;
			if (WSAIoctl(client, SIO_KEEPALIVE_VALS, &ka, sizeof(ka), NULL, 0, &ret, NULL, NULL) == SOCKET_ERROR) {
				closesocket(client);
				return nullptr;
			}

			auto newStreamBuffer = std::make_unique<TCPStreamBuffer>(Socket(SocketToI64(client)));
			newStreamBuffer->connectCalled = true;
			return std::make_unique<TCPStream>(std::move(newStreamBuffer));
		}
	}

	void TCPListener::reset(const NetEndpoint& local)
	{
		s.reset();
		status_ = TCPListenerStatus::Error;
		local_ = {};
		if (!local.valid()) return;

		SOCKET sock = INVALID_SOCKET;
		if (local.addrFamily() == AddressFamily::IPv4)
		{
			sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
			if (sock == INVALID_SOCKET) return;

			auto addr = std::get<sockaddr_in>(*EndpointToSockaddr(local));

			if (bind(sock, (sockaddr*)&addr, sizeof(sockaddr_in)) == SOCKET_ERROR)
			{
				closesocket(sock);
				return;
			}
		}
		else
		{
			sock = socket(AF_INET6, SOCK_STREAM, IPPROTO_TCP);
			if (sock == INVALID_SOCKET) return;

			auto addr = std::get<sockaddr_in6>(*EndpointToSockaddr(local));

			if (local.addrFamily() == AddressFamily::IPv6)
			{
				int opt = 0;
				if (setsockopt(sock, IPPROTO_IPV6, IPV6_V6ONLY, (char*)&opt, sizeof(opt)) == SOCKET_ERROR)
				{
					closesocket(sock);
					return;
				}
			}

			if (bind(sock, (sockaddr*)&addr, sizeof(sockaddr_in6)) == SOCKET_ERROR)
			{
				closesocket(sock);
				return;
			}
		}

		u_long mode = 1;
		if (ioctlsocket(sock, FIONBIO, &mode) == SOCKET_ERROR)
		{
			closesocket(sock);
			return;
		}

		if (listen(sock, SOMAXCONN) == SOCKET_ERROR)
		{
			closesocket(sock);
			return;
		}

		s = Socket(sock);
		status_ = TCPListenerStatus::Listening;
		local_ = local;

		if (local.addrFamily() == AddressFamily::IPv4)
		{
			sockaddr_in actualAddr;
			socklen_t len = sizeof(actualAddr);
			if (getsockname(sock, (sockaddr*)&actualAddr, &len) == 0)
			{
				char ip[INET_ADDRSTRLEN];
				inet_ntop(AF_INET, &actualAddr.sin_addr, ip, sizeof(ip));
				local_ = NetEndpoint(ip, ntohs(actualAddr.sin_port), AddressFamily::IPv4);
			}
		}
		else
		{
			sockaddr_in6 actualAddr;
			socklen_t len = sizeof(actualAddr);
			if (getsockname(sock, (sockaddr*)&actualAddr, &len) == 0)
			{
				char ip[INET6_ADDRSTRLEN];
				inet_ntop(AF_INET6, &actualAddr.sin6_addr, ip, sizeof(ip));
				local_ = NetEndpoint(ip, ntohs(actualAddr.sin6_port), AddressFamily::IPv6);
			}
		}
	}

	TCPListenerStatus TCPListener::status() {
		// 1. 检查 socket 是否有效
		if (!s.valid()) {
			status_ = TCPListenerStatus::Error;
			return status_;
		}

		SOCKET listenSock = I64ToSocket(s.get());
		if (listenSock == INVALID_SOCKET) {
			status_ = TCPListenerStatus::Error;
			return status_;
		}

		// 2. 检查 socket 错误状态
		int so_error = 0;
		socklen_t len = sizeof(so_error);
		if (getsockopt(listenSock, SOL_SOCKET, SO_ERROR, (char*)&so_error, &len) != 0 || so_error != 0) {
			status_ = TCPListenerStatus::Error;
			return status_;
		}

		// 3. 检查是否仍在监听
		int optval = 0;
		len = sizeof(optval);
		if (getsockopt(listenSock, SOL_SOCKET, SO_ACCEPTCONN, (char*)&optval, &len) == 0 && optval) {
			status_ = TCPListenerStatus::Listening;
		}
		else {
			status_ = TCPListenerStatus::Error;
		}

		return status_;
	}

	void TCPListener::close()
	{
		s.reset();
		local_ = {};
		status_ = TCPListenerStatus::Error;
	}

#pragma endregion


}

#else
// POSIX

#include <ifaddrs.h>
#include <sys/select.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

namespace tideecho
{
	bool Initialize() { return true; }
	void Cleanup() {}

	NetServiceGuard::NetServiceGuard() : initialized(Initialize()) {}
	NetServiceGuard::~NetServiceGuard() { if (initialized) Cleanup(); }
	bool NetServiceGuard::valid() const { return initialized; }
	NetServiceGuard::operator bool() const { return valid(); }

	std::vector<std::pair<AddressFamily, std::string>> GetLocalIPs()
	{
		std::vector<std::pair<AddressFamily, std::string>> ips;
		struct ifaddrs* ifaddr = nullptr;
		if (getifaddrs(&ifaddr) == -1) return ips;

		for (struct ifaddrs* ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next)
		{
			if (!ifa->ifa_addr) continue;
			char ip[INET6_ADDRSTRLEN] = { 0 };
			if (ifa->ifa_addr->sa_family == AF_INET)
			{
				auto* sa = reinterpret_cast<sockaddr_in*>(ifa->ifa_addr);
				inet_ntop(AF_INET, &sa->sin_addr, ip, INET_ADDRSTRLEN);
				ips.emplace_back(AddressFamily::IPv4, ip);
			}
			else if (ifa->ifa_addr->sa_family == AF_INET6)
			{
				auto* sa = reinterpret_cast<sockaddr_in6*>(ifa->ifa_addr);
				inet_ntop(AF_INET6, &sa->sin6_addr, ip, INET6_ADDRSTRLEN);
				ips.emplace_back(AddressFamily::IPv6, ip);
			}
		}
		freeifaddrs(ifaddr);
		return ips;
	}

	NetEndpoint::NetEndpoint(const std::string& ip, uint16_t port, AddressFamily family)
	{
		switch (family)
		{
		case AddressFamily::IPv4:
		{
			sockaddr_in tmp{};
			if (inet_pton(AF_INET, ip.c_str(), &tmp.sin_addr) == 1)
			{
				char buf[INET_ADDRSTRLEN] = {};
				inet_ntop(AF_INET, &tmp.sin_addr, buf, INET_ADDRSTRLEN);
				ip_ = buf;
				port_ = port;
				family_ = family;
			}
			break;
		}
		case AddressFamily::IPv6:
		{
			sockaddr_in6 tmp{};
			if (inet_pton(AF_INET6, ip.c_str(), &tmp.sin6_addr) == 1)
			{
				char buf[INET6_ADDRSTRLEN] = {};
				inet_ntop(AF_INET6, &tmp.sin6_addr, buf, INET6_ADDRSTRLEN);
				ip_ = buf;
				port_ = port;
				family_ = family;
			}
			break;
		}
		default: break;
		}
	}

	NetEndpoint::NetEndpoint(const std::string& ip_port)
	{
		auto pos = ip_port.rfind(':');
		if (pos == std::string::npos) return;
		std::string ipPart = ip_port.substr(0, pos);
		std::string portPart = ip_port.substr(pos + 1);
		uint64_t port_raw = 0;
		try
		{
			port_raw = std::stoul(portPart);
		}
		catch (...)
		{
			return;
		}
		if (port_raw > 65535)
		{
			return;
		}
		uint16_t port = static_cast<uint16_t>(port_raw);
		if (ipPart.find(':') != std::string::npos)
		{
			sockaddr_in6 tmp{};
			if (inet_pton(AF_INET6, ipPart.c_str(), &tmp.sin6_addr) == 1)
			{
				char buf[INET6_ADDRSTRLEN] = {};
				inet_ntop(AF_INET6, &tmp.sin6_addr, buf, INET6_ADDRSTRLEN);
				ip_ = buf;
				port_ = port;
				family_ = AddressFamily::IPv6;
			}
		}
		else
		{
			sockaddr_in tmp{};
			if (inet_pton(AF_INET, ipPart.c_str(), &tmp.sin_addr) == 1)
			{
				char buf[INET_ADDRSTRLEN] = {};
				inet_ntop(AF_INET, &tmp.sin_addr, buf, INET_ADDRSTRLEN);
				ip_ = buf;
				port_ = port;
				family_ = AddressFamily::IPv4;
			}
		}
	}

	void Socket::reset() noexcept
	{
		if (s == invalid_socket) return;
		int native = I64ToSocket(s);
		::close(native);
		s = invalid_socket;
	}

	// ---------- TCPStreamBuffer ----------
	TCPStreamBuffer::TCPStreamBuffer(NetEndpoint remote, NetEndpoint local)
	{
		setp(reinterpret_cast<char*>(writeBuf.data()), reinterpret_cast<char*>(writeBuf.data()) + writeBuf.size());
		if (!remote.valid() && !local.valid())
		{
			status_ = TCPStreamStatus::Error;
			return;
		}

		AddressFamily initFamily = local.valid() ? local.addrFamily() : remote.addrFamily();
		int af = (initFamily == AddressFamily::IPv4) ? AF_INET : AF_INET6;
		int sock = socket(af, SOCK_STREAM, IPPROTO_TCP);
		if (sock == -1)
		{
			status_ = TCPStreamStatus::Error;
			return;
		}

		// 设为非阻塞
		int flags = fcntl(sock, F_GETFL, 0);
		if (flags == -1 || fcntl(sock, F_SETFL, flags | O_NONBLOCK) == -1)
		{
			::close(sock);
			status_ = TCPStreamStatus::Error;
			return;
		}

		family = initFamily;

		if (local.valid())
		{
			auto addrOpt = EndpointToSockaddr(local);
			if (addrOpt.has_value())
			{
				bool bound = false;
				if (local.addrFamily() == AddressFamily::IPv4)
				{
					auto& addr = std::get<sockaddr_in>(*addrOpt);
					bound = (bind(sock, (sockaddr*)&addr, sizeof(addr)) == 0);
				}
				else
				{
					auto& addr = std::get<sockaddr_in6>(*addrOpt);
					bound = (bind(sock, (sockaddr*)&addr, sizeof(addr)) == 0);
				}
		if (!bound)
			{
				local_ = {};
				if (!remote.valid())
				{
					::close(sock);
					status_ = TCPStreamStatus::Error;
					return;
				}
				if (remote.addrFamily() != family)
				{
					::close(sock);
					af = (remote.addrFamily() == AddressFamily::IPv4) ? AF_INET : AF_INET6;
					sock = socket(af, SOCK_STREAM, IPPROTO_TCP);
					if (sock == -1)
					{
						status_ = TCPStreamStatus::Error;
						return;
					}
					flags = fcntl(sock, F_GETFL, 0);
					if (flags == -1 || fcntl(sock, F_SETFL, flags | O_NONBLOCK) == -1)
					{
						::close(sock);
						status_ = TCPStreamStatus::Error;
						return;
					}
					family = remote.addrFamily();
				}
			}
			}
			else
			{
				local_ = {};
			}
		}

		int keepalive_enabled = 1;
		if (setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE,
			&keepalive_enabled, sizeof(keepalive_enabled)) == -1) {
			status_ = TCPStreamStatus::Error;
			::close(sock);
			return;
		}
		else {
			int idle_sec = 20;      // 空闲超时：20 秒
			int interval_sec = 5;   // 探测间隔：5 秒
			int cnt = 3;            // 连续失败 3 次后判定断开

			if (setsockopt(sock, IPPROTO_TCP, TCP_KEEPIDLE,
				&idle_sec, sizeof(idle_sec)) == -1) {
				status_ = TCPStreamStatus::Error;
				::close(sock);
				return;
			}
			if (setsockopt(sock, IPPROTO_TCP, TCP_KEEPINTVL,
				&interval_sec, sizeof(interval_sec)) == -1) {
				status_ = TCPStreamStatus::Error;
				::close(sock);
				return;
			}
			if (setsockopt(sock, IPPROTO_TCP, TCP_KEEPCNT,
				&cnt, sizeof(cnt)) == -1) {
				status_ = TCPStreamStatus::Error;
				::close(sock);
				return;
			}
		}

		s = Socket(SocketToI64(sock));

		if (remote.valid() && (!local_.valid() || remote.addrFamily() == family))
			connect(remote, 0);
		else
			remote_ = {};

		sockaddr_storage lAddr = {};
		socklen_t len = sizeof(sockaddr_storage);
		if (getsockname(sock, (sockaddr*)(&lAddr), &len) == -1)
		{
			local_ = {};
		}
		else
		{
			if (lAddr.ss_family == AF_INET)
			{
				sockaddr_in* pAddr = (sockaddr_in*)&lAddr;
				char ip[INET_ADDRSTRLEN] = {};
				if (inet_ntop(AF_INET, &(pAddr->sin_addr), ip, INET_ADDRSTRLEN) == nullptr)
				{
					local_ = {};
				}
				else
				{
					local_ = NetEndpoint{ ip, ntohs(pAddr->sin_port), AddressFamily::IPv4 };
				}
			}
			else if (lAddr.ss_family == AF_INET6)
			{
				sockaddr_in6* pAddr = (sockaddr_in6*)&lAddr;
				char ip[INET6_ADDRSTRLEN] = {};
				if (inet_ntop(AF_INET6, &(pAddr->sin6_addr), ip, INET6_ADDRSTRLEN) == nullptr)
				{
					local_ = {};
				}
				else
				{
					local_ = NetEndpoint{ ip, ntohs(pAddr->sin6_port), AddressFamily::IPv6 };
				}
			}
			else
			{
				local_ = {};
			}
		}
	}

	TCPStreamBuffer::TCPStreamBuffer(Socket&& sock) : s(std::move(sock))
	{
		setp(reinterpret_cast<char*>(writeBuf.data()), reinterpret_cast<char*>(writeBuf.data()) + writeBuf.size());
		if (!s.valid())
		{
			status_ = TCPStreamStatus::Error;
			return;
		}

		int native = I64ToSocket(s.get());
		// 设为非阻塞
		int flags = fcntl(native, F_GETFL, 0);
		if (flags != -1) fcntl(native, F_SETFL, flags | O_NONBLOCK);

		struct sockaddr_storage ss;
		socklen_t len = sizeof(ss);
		if (getsockname(native, (sockaddr*)&ss, &len) == 0)
		{
			if (ss.ss_family == AF_INET)
			{
				family = AddressFamily::IPv4;
				auto* addr = (sockaddr_in*)&ss;
				char ip[INET_ADDRSTRLEN];
				inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip));
				local_ = NetEndpoint(ip, ntohs(addr->sin_port), AddressFamily::IPv4);
			}
			else if (ss.ss_family == AF_INET6)
			{
				family = AddressFamily::IPv6;
				auto* addr = (sockaddr_in6*)&ss;
				char ip[INET6_ADDRSTRLEN];
				inet_ntop(AF_INET6, &addr->sin6_addr, ip, sizeof(ip));
				local_ = NetEndpoint(ip, ntohs(addr->sin6_port), AddressFamily::IPv6);
			}
		}

		len = sizeof(ss);
		if (getpeername(native, (sockaddr*)&ss, &len) == 0)
		{
			if (ss.ss_family == AF_INET)
			{
				auto* addr = (sockaddr_in*)&ss;
				char ip[INET_ADDRSTRLEN];
				inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip));
				remote_ = NetEndpoint(ip, ntohs(addr->sin_port), AddressFamily::IPv4);
			}
			else if (ss.ss_family == AF_INET6)
			{
				auto* addr = (sockaddr_in6*)&ss;
				char ip[INET6_ADDRSTRLEN];
				inet_ntop(AF_INET6, &addr->sin6_addr, ip, sizeof(ip));
				remote_ = NetEndpoint(ip, ntohs(addr->sin6_port), AddressFamily::IPv6);
			}
		}

		status();
	}

	TCPStreamBuffer::TCPStreamBuffer(TCPStreamBuffer&& other) noexcept
		: std::streambuf(std::move(other)),
		s(std::move(other.s)),
		readBuf(std::move(other.readBuf)),
		writeBuf(std::move(other.writeBuf)),
		remote_(std::move(other.remote_)),
		local_(std::move(other.local_)),
		status_(other.status_),
		family(other.family)
	{
		other.status_ = TCPStreamStatus::Error;
		other.family = AddressFamily::Unknown;
	}

	TCPStreamBuffer& TCPStreamBuffer::operator=(TCPStreamBuffer&& other) noexcept
	{
		if (this != &other)
		{
			std::streambuf::operator=(std::move(other));
			s = std::move(other.s);
			readBuf = std::move(other.readBuf);
			writeBuf = std::move(other.writeBuf);
			remote_ = std::move(other.remote_);
			local_ = std::move(other.local_);
			status_ = other.status_;
			family = other.family;
			other.status_ = TCPStreamStatus::Error;
			other.family = AddressFamily::Unknown;
		}
		return *this;
	}

	void TCPStreamBuffer::reset()
	{
		s.reset();
		remote_ = {};
		local_ = {};
		status_ = TCPStreamStatus::Error;
		family = AddressFamily::Unknown;
	}

	TCPStreamStatus TCPStreamBuffer::connect(NetEndpoint remote, int64_t timeout_ms)
	{
		status();
		if (status_ != TCPStreamStatus::Idle)
			return TCPStreamStatus::Error;

		if (remote.addrFamily() != family)
			return TCPStreamStatus::Error;

		if (!s.valid())
		{
			reset();
			return TCPStreamStatus::Error;
		}

		int sock = I64ToSocket(s.get());
		auto addrOpt = EndpointToSockaddr(remote);
		if (!addrOpt)
			return TCPStreamStatus::Error;

		sockaddr* addrPtr = nullptr;
		int addrLen = 0;
		if (family == AddressFamily::IPv4)
		{
			auto& addr = std::get<sockaddr_in>(*addrOpt);
			addrPtr = reinterpret_cast<sockaddr*>(&addr);
			addrLen = sizeof(addr);
		}
		else
		{
			auto& addr = std::get<sockaddr_in6>(*addrOpt);
			addrPtr = reinterpret_cast<sockaddr*>(&addr);
			addrLen = sizeof(addr);
		}

		if (timeout_ms < 0)
		{
			// 临时阻塞
			int flags = fcntl(sock, F_GETFL, 0);
			if (flags != -1) fcntl(sock, F_SETFL, flags & ~O_NONBLOCK);
			connectCalled = true;
			int ret = ::connect(sock, addrPtr, addrLen);
			if (flags != -1) fcntl(sock, F_SETFL, flags);
			if (ret == 0)
			{
				remote_ = std::move(remote);
				status_ = TCPStreamStatus::Connected;
				return TCPStreamStatus::Connected;
			}
			else
			{
				reset();
				return TCPStreamStatus::Error;
			}
		}

		connectCalled = true;
		int ret = ::connect(sock, addrPtr, addrLen);
		if (ret == 0)
		{
			remote_ = std::move(remote);
			status_ = TCPStreamStatus::Connected;
			return TCPStreamStatus::Connected;
		}
		if (errno != EINPROGRESS)
		{
			reset();
			return TCPStreamStatus::Error;
		}

		if (timeout_ms == 0)
		{
			remote_ = std::move(remote);
			status_ = TCPStreamStatus::Connecting;
			return TCPStreamStatus::Connecting;
		}

		using namespace std::chrono;
		auto start = steady_clock::now();
		auto timeout_dur = milliseconds(timeout_ms);
		int delay_us = 100;
		int step = 0;

		while (true)
		{
			auto now = steady_clock::now();
			auto elapsed = now - start;
			if (elapsed >= timeout_dur)
			{
				remote_ = std::move(remote);
				status_ = TCPStreamStatus::Connecting;
				return TCPStreamStatus::Connecting;
			}

			int wait_us = delay_us;
			auto remain_us = duration_cast<microseconds>(timeout_dur - elapsed).count();
			if (wait_us > remain_us)
				wait_us = static_cast<int>(remain_us);

			fd_set writefds;
			FD_ZERO(&writefds);
			FD_SET(sock, &writefds);
			struct timeval tv;
			tv.tv_sec = wait_us / 1000000;
			tv.tv_usec = wait_us % 1000000;

			int sel = select(sock + 1, nullptr, &writefds, nullptr, &tv);
			if (sel == -1)
			{
				reset();
				return TCPStreamStatus::Error;
			}
			if (sel == 0)
			{
				if (step == 0) delay_us = 200;
				else if (step == 1) delay_us = 400;
				else delay_us = 1000;
				step++;
				continue;
			}

			int so_error = 0;
			socklen_t len = sizeof(so_error);
			if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &so_error, &len) == 0)
			{
				if (so_error == 0)
				{
					remote_ = std::move(remote);
					status_ = TCPStreamStatus::Connected;
					return TCPStreamStatus::Connected;
				}
				else
				{
					reset();
					return TCPStreamStatus::Error;
				}
			}
			else
			{
				reset();
				return TCPStreamStatus::Error;
			}
		}
	}

	TCPStreamStatus TCPStreamBuffer::status()
	{
		// 1. 若已为错误状态，直接返回
		if (status_ == TCPStreamStatus::Error)
			return TCPStreamStatus::Error;

		// 2. 获取套接字句柄，校验有效性（POSIX 下为 int，无效为 -1）
		int sock = I64ToSocket(s.get());
		if (sock < 0) {
			status_ = TCPStreamStatus::Error;
			s.reset();
			return status_;
		}

		// 3. 如果从未调用过 connect，直接返回 Idle
		if (!connectCalled) {
			status_ = TCPStreamStatus::Idle;
			return status_;
		}

		// 4. 检查连接过程状态（通过 SO_ERROR）
		int error = 0;
		socklen_t optlen = sizeof(error);
		int ret = getsockopt(sock, SOL_SOCKET, SO_ERROR, &error, &optlen);
		if (ret == -1) {
			status_ = TCPStreamStatus::Error;
			s.reset();
			return status_;
		}

		if (error == EINPROGRESS) {
			status_ = TCPStreamStatus::Connecting;
			return status_;
		}
		else if (error != 0) {
			status_ = TCPStreamStatus::Error;
			s.reset();
			return status_;
		}

		// 5. 检测已建立连接是否仍然存活（被动断开检测）
		fd_set readfds;
		FD_ZERO(&readfds);
		FD_SET(sock, &readfds);
		struct timeval tv = { 0, 0 };
		int selRet = select(sock + 1, &readfds, nullptr, nullptr, &tv);

		if (selRet == -1) {
			status_ = TCPStreamStatus::Error;
			s.reset();
			return status_;
		}

		if (selRet == 0) {
			status_ = TCPStreamStatus::Connected;
			return status_;
		}

		char buf[1];
		int recvRet = ::recv(sock, buf, 1, MSG_PEEK);
		if (recvRet == 0) {
			status_ = TCPStreamStatus::Error;
			s.reset();
			return status_;
		}
		else if (recvRet == -1) {
			int err = errno;
			if (err == EAGAIN || err == EWOULDBLOCK) {
				status_ = TCPStreamStatus::Connected;
			}
			else {
				status_ = TCPStreamStatus::Error;
				s.reset();
			}
			return status_;
		}
		else {
			status_ = TCPStreamStatus::Connected;
			return status_;
		}
	}

	int64_t TCPStreamBuffer::send(std::span<const uint8_t> data, int64_t timeout_ms)
	{
		status();
		if (data.empty()) return 0;
		if (status_ != TCPStreamStatus::Connected) return -1;

		int sock = I64ToSocket(s.get());
		if (sock == -1)
		{
			status_ = TCPStreamStatus::Error;
			return -1;
		}

		if (timeout_ms < 0)
		{
			int flags = fcntl(sock, F_GETFL, 0);
			if (flags != -1) fcntl(sock, F_SETFL, flags & ~O_NONBLOCK);
			ssize_t ret = ::send(sock, data.data(), data.size(), 0);
			if (flags != -1) fcntl(sock, F_SETFL, flags);
			if (ret > 0) return ret;
			if (ret == 0)
			{
				status_ = TCPStreamStatus::Error;
				s.reset();
				return 0;
			}
			if (errno == EWOULDBLOCK || errno == EAGAIN)
				return -1;
			status_ = TCPStreamStatus::Error;
			s.reset();
			return -1;
		}

		ssize_t ret = ::send(sock, data.data(), data.size(), 0);
		if (ret > 0) return ret;
		if (ret == 0)
		{
			status_ = TCPStreamStatus::Error;
			s.reset();
			return 0;
		}
		if (errno != EWOULDBLOCK && errno != EAGAIN)
		{
			status_ = TCPStreamStatus::Error;
			s.reset();
			return -1;
		}
		if (timeout_ms == 0) return -1;

		using namespace std::chrono;
		auto start = steady_clock::now();
		auto timeout_dur = milliseconds(timeout_ms);
		int delay_us = 100;
		int step = 0;

		while (true)
		{
			auto now = steady_clock::now();
			auto elapsed = now - start;
			if (elapsed >= timeout_dur) return -1;

			int wait_us = delay_us;
			auto remain_us = duration_cast<microseconds>(timeout_dur - elapsed).count();
			if (wait_us > remain_us) wait_us = static_cast<int>(remain_us);

			fd_set writefds;
			FD_ZERO(&writefds);
			FD_SET(sock, &writefds);
			struct timeval tv;
			tv.tv_sec = wait_us / 1000000;
			tv.tv_usec = wait_us % 1000000;

			int sel = select(sock + 1, nullptr, &writefds, nullptr, &tv);
			if (sel == -1)
			{
				status_ = TCPStreamStatus::Error;
				s.reset();
				return -1;
			}
			if (sel == 0)
			{
				if (step == 0) delay_us = 200;
				else if (step == 1) delay_us = 400;
				else delay_us = 1000;
				step++;
				continue;
			}

			ret = ::send(sock, data.data(), data.size(), 0);
			if (ret > 0) return ret;
			if (ret == 0)
			{
				status_ = TCPStreamStatus::Error;
				s.reset();
				return 0;
			}
			if (errno == EWOULDBLOCK || errno == EAGAIN) continue;
			status_ = TCPStreamStatus::Error;
			s.reset();
			return -1;
		}
	}

	int64_t TCPStreamBuffer::send(const uint8_t* buffer, size_t size, int64_t timeout_ms)
	{
		return send(std::span<const uint8_t>(buffer, size), timeout_ms);
	}

	int64_t TCPStreamBuffer::recv(std::span<uint8_t> buffer, int64_t timeout_ms)
	{
		status();
		if (buffer.empty()) return 0;
		if (status_ != TCPStreamStatus::Connected) return -1;

		int sock = I64ToSocket(s.get());
		if (sock == -1)
		{
			status_ = TCPStreamStatus::Error;
			return -1;
		}

		if (timeout_ms < 0)
		{
			int flags = fcntl(sock, F_GETFL, 0);
			if (flags != -1) fcntl(sock, F_SETFL, flags & ~O_NONBLOCK);
			ssize_t ret = ::recv(sock, buffer.data(), buffer.size(), 0);
			if (flags != -1) fcntl(sock, F_SETFL, flags);
			if (ret > 0) return ret;
			if (ret == 0)
			{
				status_ = TCPStreamStatus::Error;
				s.reset();
				return 0;
			}
			status_ = TCPStreamStatus::Error;
			s.reset();
			return -1;
		}

		ssize_t ret = ::recv(sock, buffer.data(), buffer.size(), 0);
		if (ret > 0) return ret;
		if (ret == 0)
		{
			status_ = TCPStreamStatus::Error;
			s.reset();
			return 0;
		}
		if (errno != EWOULDBLOCK && errno != EAGAIN)
		{
			status_ = TCPStreamStatus::Error;
			s.reset();
			return -1;
		}
		if (timeout_ms == 0) return -1;

		using namespace std::chrono;
		auto start = steady_clock::now();
		auto timeout_dur = milliseconds(timeout_ms);
		int delay_us = 100;
		int step = 0;

		while (true)
		{
			auto now = steady_clock::now();
			auto elapsed = now - start;
			if (elapsed >= timeout_dur) return -1;

			int wait_us = delay_us;
			auto remain_us = duration_cast<microseconds>(timeout_dur - elapsed).count();
			if (wait_us > remain_us) wait_us = static_cast<int>(remain_us);

			fd_set readfds;
			FD_ZERO(&readfds);
			FD_SET(sock, &readfds);
			struct timeval tv;
			tv.tv_sec = wait_us / 1000000;
			tv.tv_usec = wait_us % 1000000;

			int sel = select(sock + 1, &readfds, nullptr, nullptr, &tv);
			if (sel == -1)
			{
				status_ = TCPStreamStatus::Error;
				s.reset();
				return -1;
			}
			if (sel == 0)
			{
				if (step == 0) delay_us = 200;
				else if (step == 1) delay_us = 400;
				else delay_us = 1000;
				step++;
				continue;
			}

			ret = ::recv(sock, buffer.data(), buffer.size(), 0);
			if (ret > 0) return ret;
			if (ret == 0)
			{
				status_ = TCPStreamStatus::Error;
				s.reset();
				return 0;
			}
			if (errno == EWOULDBLOCK || errno == EAGAIN) continue;
			status_ = TCPStreamStatus::Error;
			s.reset();
			return -1;
		}
	}

	int64_t TCPStreamBuffer::recv(uint8_t* buffer, size_t size, int64_t timeout_ms)
	{
		return recv(std::span<uint8_t>(buffer, size), timeout_ms);
	}

	std::streambuf::int_type TCPStreamBuffer::underflow()
	{
		if (gptr() < egptr())
			return traits_type::to_int_type(*gptr());

		int64_t ret = recv({ readBuf.data(), readBuf.size() }, -1);
		if (ret <= 0) return traits_type::eof();

		setg(reinterpret_cast<char*>(readBuf.data()), reinterpret_cast<char*>(readBuf.data()), reinterpret_cast<char*>(readBuf.data()) + static_cast<size_t>(ret));
		return traits_type::to_int_type(*gptr());
	}

	std::streambuf::int_type TCPStreamBuffer::overflow(int_type c)
	{
		if (pbase() && pptr() > pbase())
		{
			size_t size = static_cast<size_t>(pptr() - pbase());
			int64_t sent = send({ reinterpret_cast<uint8_t*>(pbase()), size }, -1);
			if (sent != static_cast<int64_t>(size)) return traits_type::eof();
			pbump(-static_cast<int>(size));
		}
		if (!traits_type::eq_int_type(c, traits_type::eof()))
		{
			if (pptr() < epptr())
			{
				*pptr() = traits_type::to_char_type(c);
				pbump(1);
			}
			else
			{
				uint8_t ch = traits_type::to_char_type(c);
				if (send({ &ch, 1 }, -1) != 1) return traits_type::eof();
			}
		}
		return traits_type::not_eof(c);
	}

	int TCPStreamBuffer::sync()
	{
		if (pbase() && pptr() > pbase())
		{
			size_t size = static_cast<size_t>(pptr() - pbase());
			if (send({ reinterpret_cast<uint8_t*>(pbase()), size }, -1) != static_cast<int64_t>(size)) return -1;
			pbump(-static_cast<int>(size));
		}
		return 0;
	}

	// ---------- TCPListener ----------
	TCPListener::TCPListener(const NetEndpoint& local)
	{
		if (!local.valid()) return;
		int af = (local.addrFamily() == AddressFamily::IPv4) ? AF_INET : AF_INET6;
		int sock = socket(af, SOCK_STREAM, IPPROTO_TCP);
		if (sock == -1) return;

		int flags = fcntl(sock, F_GETFL, 0);
		if (flags == -1 || fcntl(sock, F_SETFL, flags | O_NONBLOCK) == -1)
		{
			::close(sock);
			return;
		}

		auto addrOpt = EndpointToSockaddr(local);
		if (!addrOpt) { ::close(sock); return; }

		if (local.addrFamily() == AddressFamily::IPv6)
		{
			int opt = 0;
			if (setsockopt(sock, IPPROTO_IPV6, IPV6_V6ONLY, (char*)&opt, sizeof(opt)) == -1)
			{
				::close(sock);
				return;
			}
		}

		bool bound = false;
		if (local.addrFamily() == AddressFamily::IPv4)
		{
			auto& addr = std::get<sockaddr_in>(*addrOpt);
			bound = (bind(sock, (sockaddr*)&addr, sizeof(addr)) == 0);
		}
		else
		{
			auto& addr = std::get<sockaddr_in6>(*addrOpt);
			bound = (bind(sock, (sockaddr*)&addr, sizeof(addr)) == 0);
		}
		if (!bound) { ::close(sock); return; }

		if (listen(sock, SOMAXCONN) != 0) { ::close(sock); return; }

		s = Socket(SocketToI64(sock));
		status_ = TCPListenerStatus::Listening;
		local_ = local;

		struct sockaddr_storage ss;
		socklen_t len = sizeof(ss);
		if (getsockname(sock, (sockaddr*)&ss, &len) == 0)
		{
			if (ss.ss_family == AF_INET)
			{
				auto* addr = (sockaddr_in*)&ss;
				char ip[INET_ADDRSTRLEN];
				inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip));
				local_ = NetEndpoint(ip, ntohs(addr->sin_port), AddressFamily::IPv4);
			}
			else if (ss.ss_family == AF_INET6)
			{
				auto* addr = (sockaddr_in6*)&ss;
				char ip[INET6_ADDRSTRLEN];
				inet_ntop(AF_INET6, &addr->sin6_addr, ip, sizeof(ip));
				local_ = NetEndpoint(ip, ntohs(addr->sin6_port), AddressFamily::IPv6);
			}
		}
	}

	std::unique_ptr<TCPStream> TCPListener::accept(int64_t timeout_ms)
	{
		if (!valid() || status_ != TCPListenerStatus::Listening) return nullptr;
		int listenSock = I64ToSocket(s.get());
		if (listenSock == -1) return nullptr;
		if (timeout_ms < 0)
		{
			int flags = fcntl(listenSock, F_GETFL, 0);
			if (flags != -1) fcntl(listenSock, F_SETFL, flags & ~O_NONBLOCK);
			int client = ::accept(listenSock, nullptr, nullptr);
			if (flags != -1) fcntl(listenSock, F_SETFL, flags);
			if (client == -1) return nullptr;
			// 设置 client 非阻塞
			int cflags = fcntl(client, F_GETFL, 0);
			if (cflags != -1) fcntl(client, F_SETFL, cflags | O_NONBLOCK);

			int keepalive_enabled = 1;
			if (setsockopt(client, SOL_SOCKET, SO_KEEPALIVE,
				&keepalive_enabled, sizeof(keepalive_enabled)) == -1) {
				::close(client);
				return nullptr;
			}
			else {
				int idle_sec = 20;      // 空闲超时：20 秒
				int interval_sec = 5;   // 探测间隔：5 秒
				int cnt = 3;            // 连续失败 3 次后判定断开

				if (setsockopt(client, IPPROTO_TCP, TCP_KEEPIDLE,
					&idle_sec, sizeof(idle_sec)) == -1) {
					::close(client);
					return nullptr;
				}
				if (setsockopt(client, IPPROTO_TCP, TCP_KEEPINTVL,
					&interval_sec, sizeof(interval_sec)) == -1) {
					::close(client);
					return nullptr;
				}
				if (setsockopt(client, IPPROTO_TCP, TCP_KEEPCNT,
					&cnt, sizeof(cnt)) == -1) {
					::close(client);
					return nullptr;
				}
			}

			auto newStreamBuffer = std::make_unique<TCPStreamBuffer>(Socket(SocketToI64(client)));
			newStreamBuffer->connectCalled = true;
			return std::make_unique<TCPStream>(std::move(newStreamBuffer));
		}

		if (timeout_ms == 0)
		{
			int client = ::accept(listenSock, nullptr, nullptr);
			if (client != -1)
			{
				int cflags = fcntl(client, F_GETFL, 0);
				if (cflags != -1) fcntl(client, F_SETFL, cflags | O_NONBLOCK);

				int keepalive_enabled = 1;
				if (setsockopt(client, SOL_SOCKET, SO_KEEPALIVE,
					&keepalive_enabled, sizeof(keepalive_enabled)) == -1) {
					::close(client);
					return nullptr;
				}
				else {
					int idle_sec = 20;      // 空闲超时：20 秒
					int interval_sec = 5;   // 探测间隔：5 秒
					int cnt = 3;            // 连续失败 3 次后判定断开

					if (setsockopt(client, IPPROTO_TCP, TCP_KEEPIDLE,
						&idle_sec, sizeof(idle_sec)) == -1) {
						::close(client);
						return nullptr;
					}
					if (setsockopt(client, IPPROTO_TCP, TCP_KEEPINTVL,
						&interval_sec, sizeof(interval_sec)) == -1) {
						::close(client);
						return nullptr;
					}
					if (setsockopt(client, IPPROTO_TCP, TCP_KEEPCNT,
						&cnt, sizeof(cnt)) == -1) {
						::close(client);
						return nullptr;
					}
				}

				auto newStreamBuffer = std::make_unique<TCPStreamBuffer>(Socket(SocketToI64(client)));
				newStreamBuffer->connectCalled = true;
				return std::make_unique<TCPStream>(std::move(newStreamBuffer));
			}
			if (errno == EWOULDBLOCK || errno == EAGAIN) return nullptr;
			return nullptr;
		}

		using namespace std::chrono;
		auto start = steady_clock::now();
		auto timeout_dur = milliseconds(timeout_ms);
		int delay_us = 100;
		int step = 0;

		while (true)
		{
			auto now = steady_clock::now();
			auto elapsed = now - start;
			if (elapsed >= timeout_dur) return nullptr;

			int wait_us = delay_us;
			auto remain_us = duration_cast<microseconds>(timeout_dur - elapsed).count();
			if (wait_us > remain_us) wait_us = static_cast<int>(remain_us);

			fd_set readfds;
			FD_ZERO(&readfds);
			FD_SET(listenSock, &readfds);
			struct timeval tv;
			tv.tv_sec = wait_us / 1000000;
			tv.tv_usec = wait_us % 1000000;

			int sel = select(listenSock + 1, &readfds, nullptr, nullptr, &tv);
			if (sel == -1) return nullptr;
			if (sel == 0)
			{
				if (step == 0) delay_us = 200;
				else if (step == 1) delay_us = 400;
				else delay_us = 1000;
				step++;
				continue;
			}

			int client = ::accept(listenSock, nullptr, nullptr);
			if (client == -1)
			{
				if (errno == EWOULDBLOCK || errno == EAGAIN) continue;
				return nullptr;
			}
			int cflags = fcntl(client, F_GETFL, 0);
			if (cflags != -1) fcntl(client, F_SETFL, cflags | O_NONBLOCK);

			int keepalive_enabled = 1;
			if (setsockopt(client, SOL_SOCKET, SO_KEEPALIVE,
				&keepalive_enabled, sizeof(keepalive_enabled)) == -1) {
				::close(client);
				return nullptr;
			}
			else {
				int idle_sec = 20;      // 空闲超时：20 秒
				int interval_sec = 5;   // 探测间隔：5 秒
				int cnt = 3;            // 连续失败 3 次后判定断开

				if (setsockopt(client, IPPROTO_TCP, TCP_KEEPIDLE,
					&idle_sec, sizeof(idle_sec)) == -1) {
					::close(client);
					return nullptr;
				}
				if (setsockopt(client, IPPROTO_TCP, TCP_KEEPINTVL,
					&interval_sec, sizeof(interval_sec)) == -1) {
					::close(client);
					return nullptr;
				}
				if (setsockopt(client, IPPROTO_TCP, TCP_KEEPCNT,
					&cnt, sizeof(cnt)) == -1) {
					::close(client);
					return nullptr;
				}
			}

			auto newStreamBuffer = std::make_unique<TCPStreamBuffer>(Socket(SocketToI64(client)));
			newStreamBuffer->connectCalled = true;
			return std::make_unique<TCPStream>(std::move(newStreamBuffer));
		}
	}

	void TCPListener::reset(const NetEndpoint& local)
	{
		close();
		if (!local.valid()) return;

		int af = (local.addrFamily() == AddressFamily::IPv4) ? AF_INET : AF_INET6;
		int sock = socket(af, SOCK_STREAM, IPPROTO_TCP);
		if (sock == -1) return;

		int flags = fcntl(sock, F_GETFL, 0);
		if (flags == -1 || fcntl(sock, F_SETFL, flags | O_NONBLOCK) == -1)
		{
			::close(sock);
			return;
		}

		auto addrOpt = EndpointToSockaddr(local);
		if (!addrOpt) { ::close(sock); return; }

		if (local.addrFamily() == AddressFamily::IPv6)
		{
			int opt = 0;
			if (setsockopt(sock, IPPROTO_IPV6, IPV6_V6ONLY, (char*)&opt, sizeof(opt)) == -1)
			{
				::close(sock);
				return;
			}
		}

		if (local.addrFamily() == AddressFamily::IPv4)
		{
			auto& addr = std::get<sockaddr_in>(*addrOpt);
			if (bind(sock, (sockaddr*)&addr, sizeof(addr)) != 0) { ::close(sock); return; }
		}
		else
		{
			auto& addr = std::get<sockaddr_in6>(*addrOpt);
			if (bind(sock, (sockaddr*)&addr, sizeof(addr)) != 0) { ::close(sock); return; }
		}

		if (listen(sock, SOMAXCONN) != 0) { ::close(sock); return; }

		s = Socket(SocketToI64(sock));
		status_ = TCPListenerStatus::Listening;
		local_ = local;

		struct sockaddr_storage ss;
		socklen_t len = sizeof(ss);
		if (getsockname(sock, (sockaddr*)&ss, &len) == 0)
		{
			if (ss.ss_family == AF_INET)
			{
				auto* addr = (sockaddr_in*)&ss;
				char ip[INET_ADDRSTRLEN];
				inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip));
				local_ = NetEndpoint(ip, ntohs(addr->sin_port), AddressFamily::IPv4);
			}
			else if (ss.ss_family == AF_INET6)
			{
				auto* addr = (sockaddr_in6*)&ss;
				char ip[INET6_ADDRSTRLEN];
				inet_ntop(AF_INET6, &addr->sin6_addr, ip, sizeof(ip));
				local_ = NetEndpoint(ip, ntohs(addr->sin6_port), AddressFamily::IPv6);
			}
		}
	}

	TCPListenerStatus TCPListener::status()
	{
		if (!s.valid()) { status_ = TCPListenerStatus::Error; return status_; }
		int listenSock = I64ToSocket(s.get());
		if (listenSock == -1) { status_ = TCPListenerStatus::Error; return status_; }

		int so_error = 0;
		socklen_t len = sizeof(so_error);
		if (getsockopt(listenSock, SOL_SOCKET, SO_ERROR, &so_error, &len) != 0 || so_error != 0)
		{
			status_ = TCPListenerStatus::Error;
			return status_;
		}
		int optval = 0;
		len = sizeof(optval);
		if (getsockopt(listenSock, SOL_SOCKET, SO_ACCEPTCONN, &optval, &len) == 0 && optval)
			status_ = TCPListenerStatus::Listening;
		else
			status_ = TCPListenerStatus::Error;
		return status_;
	}

	void TCPListener::close()
	{
		s.reset();
		local_ = {};
		status_ = TCPListenerStatus::Error;
	}

}

#endif