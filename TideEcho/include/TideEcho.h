#pragma once

#include <memory>
#include <string>
#include <iostream>
#include <streambuf>
#include <span>
#include <string_view>
#include <vector>
#include <optional>
#include <atomic>
#include <mutex>
#include <deque>
#include <functional>
#include <unordered_map>
#include <algorithm>

namespace tideecho
{
	bool Initialize();
	void Cleanup();

	// RAII guard for net initialization and cleanup
	class NetServiceGuard
	{
	private:
		bool initialized;
	public:
		NetServiceGuard();
		~NetServiceGuard();
		bool valid() const;
		operator bool() const;

		NetServiceGuard(const NetServiceGuard&) = delete;
		NetServiceGuard& operator=(const NetServiceGuard&) = delete;
	};

	enum class AddressFamily : uint8_t
	{
		IPv4,
		IPv6,
		Unknown
	};

	class NetEndpoint
	{
	private:
		std::string ip_ = "";
		uint16_t port_ = 0;
		AddressFamily family_ = AddressFamily::Unknown;
	public:
		NetEndpoint() = default;
		NetEndpoint(const std::string& ip, uint16_t port, AddressFamily family);
		NetEndpoint(const std::string& ip_port);

		AddressFamily addrFamily() const noexcept { return family_; }

		uint16_t port() const noexcept { return port_; }
		const std::string& ip() const noexcept { return ip_; }

		std::string toString() const { return ip_ + ':' + std::to_string(port_); }

		explicit operator std::string() const { return this->toString(); }

		bool operator==(const NetEndpoint& other) const noexcept { return family_ == other.family_ ? (port_ == other.port_ && ip_ == other.ip_) : false; }
		bool operator!=(const NetEndpoint& other) const noexcept { return !(*this == other); }

		bool valid() const noexcept { return family_ != AddressFamily::Unknown; }
	};
}

namespace std
{
	template<>
	struct hash<tideecho::NetEndpoint>
	{
		size_t operator()(const tideecho::NetEndpoint& p) const
		{
			if (!p.valid()) return 0;
			size_t hash = std::hash<std::string>{}(p.ip());
			return hash ^ (std::hash<uint32_t>{}(p.port()) << 5);
		}
	};
}

namespace tideecho
{

	std::vector<std::pair<AddressFamily, std::string>> GetLocalIPs();


	enum class SocketStatus
	{
		Idle,
		Connecting,
		Connected,
		Error
	};

	class Socket
	{
	private:
		int64_t s = invalid_socket;

	public:
		static constexpr int64_t invalid_socket = -1;
		Socket() = default;
		explicit Socket(int64_t s) noexcept : s(s) {}
		Socket(const Socket& other) = delete;
		Socket(Socket&& other) noexcept : s(other.s) { other.s = invalid_socket; }
		Socket& operator=(const Socket& other) = delete;
		Socket& operator=(Socket&& other) noexcept { if (&other != this) { reset(); s = other.s; other.s = invalid_socket; } return *this; }
		bool valid() const noexcept { return s != invalid_socket; }
		int64_t get() const noexcept { return s; };
		void reset() noexcept;
		int64_t release() noexcept { int64_t r = s; s = invalid_socket; return r; };
		~Socket() noexcept { reset(); };
	};


	enum class TCPStreamStatus
	{
		Idle,
		Connecting,
		Connected,
		Error
	};

	class TCPStreamBuffer : public std::streambuf
	{
	private:
		Socket s;
		std::vector<uint8_t> readBuf = std::vector<uint8_t>(bufferSize);
		std::vector<uint8_t> writeBuf = std::vector<uint8_t>(bufferSize);

		NetEndpoint remote_ = {};
		NetEndpoint local_ = {};

		TCPStreamStatus status_ = TCPStreamStatus::Idle;
		AddressFamily family = AddressFamily::Unknown;

		bool connectCalled = false;

		friend class TCPListener;

	public:
		constexpr static size_t bufferSize = 1024;
		TCPStreamBuffer() = delete;
		TCPStreamBuffer(const TCPStreamBuffer&) = delete;
		TCPStreamBuffer(TCPStreamBuffer&& other) noexcept;
		TCPStreamBuffer(NetEndpoint remote, NetEndpoint local = {});
		TCPStreamBuffer(AddressFamily family) : TCPStreamBuffer({}, NetEndpoint{ family == AddressFamily::IPv4 ? "0.0.0.0" : "::", 0, family }) {}
		TCPStreamBuffer(Socket&& sock);

		TCPStreamBuffer& operator=(const TCPStreamBuffer&) = delete;
		TCPStreamBuffer& operator=(TCPStreamBuffer&& other) noexcept;

		int_type underflow() override;
		int sync() override;
		int_type overflow(int_type c = traits_type::eof()) override;

		TCPStreamStatus status();
		AddressFamily addrFamily() const noexcept { return family; }


		TCPStreamStatus connect(NetEndpoint remote, int64_t timeout_ms = -1);

		void reset();

		const NetEndpoint& local() const { return local_; }
		const NetEndpoint& remote() const { return remote_; }
		
		int64_t recv(std::span<uint8_t> buffer, int64_t timeout_ms = -1);
		int64_t recv(uint8_t* buffer, size_t size, int64_t timeout_ms = -1);
		int64_t send(std::span<const uint8_t> buffer, int64_t timeout_ms = -1);
		int64_t send(const uint8_t* buffer, size_t size, int64_t timeout_ms = -1);
	};

	class TCPStream : public std::iostream
	{
	private:
		std::unique_ptr<TCPStreamBuffer> buffer;
	public:
		TCPStream() = delete;
		TCPStream(NetEndpoint remote, NetEndpoint local = {});
		TCPStream(AddressFamily family) : TCPStream({}, NetEndpoint{ family == AddressFamily::IPv4 ? "0.0.0.0" : "::", 0, family }) {};
		TCPStream(std::unique_ptr<TCPStreamBuffer>&& buffer);

		TCPStream(TCPStream&& other) = delete;
		TCPStream(const TCPStream&& other) = delete;

		TCPStreamStatus connect(NetEndpoint remote, int64_t timeout_ms = -1);

		bool is_open();
		TCPStreamStatus status();

		const NetEndpoint& local() const { return buffer->local(); }
		const NetEndpoint& remote() const { return buffer->remote(); }

		int64_t recv(std::span<uint8_t> buffer, int64_t timeout_ms = -1);
		int64_t recv(uint8_t* buffer, size_t size, int64_t timeout_ms = -1);
		int64_t send(std::span<const uint8_t> buffer, int64_t timeout_ms = -1);
		int64_t send(const uint8_t* buffer, size_t size, int64_t timeout_ms = -1);
	};


	enum class TCPListenerStatus
	{
		Listening,
		Error
	};

	class TCPListener
	{
	private:
		Socket s;
		TCPListenerStatus status_ = TCPListenerStatus::Error;
		NetEndpoint local_ = {};
		
	public:
		TCPListener(const NetEndpoint& local);
		TCPListener(uint16_t port) : TCPListener(NetEndpoint("::", port, AddressFamily::IPv6)) {}

		const NetEndpoint& local() const { return local_; }
		std::unique_ptr<TCPStream> accept(int64_t timeout_ms = -1);

		void close();
		void reset(const NetEndpoint& local = {});
		TCPListenerStatus status();
		AddressFamily family() const { return local_.addrFamily(); }
		bool valid() { return status() == TCPListenerStatus::Listening; };
	};


	enum class AsyncSendStatus
	{
		InQueue,
		Sending,
		Sent,
		Failed
	};

	class AsyncSendResult
	{
	private:
		friend class TCPClient;
		friend class TCPServer;

		std::shared_ptr<std::atomic<AsyncSendStatus>> status_;

	public:
		AsyncSendResult(std::shared_ptr<std::atomic<AsyncSendStatus>> status) : status_(status) {}
		AsyncSendStatus status() const { return *status_; }
		bool inQueue() const { return *status_ == AsyncSendStatus::InQueue; }
		bool sending() const { return *status_ == AsyncSendStatus::Sending; }
		bool sent() const { return *status_ == AsyncSendStatus::Sent; }
		bool failed() const { return *status_ == AsyncSendStatus::Failed; }
	};


	struct SendQueueItem
	{
		std::vector<uint8_t> data;
		std::span<const uint8_t> dataRef;
		std::shared_ptr<std::atomic<AsyncSendStatus>> status;
	};

	class TCPConnectionProcessor
	{
	private:
		std::unique_ptr<TCPStream> stream;

		std::function<std::optional<SendQueueItem>(NetEndpoint)> getSendData;
		std::function<void(std::vector<uint8_t>&&, NetEndpoint)> pullRecvData;
		std::function<void(NetEndpoint)> streamError;

		using head_t = uint32_t;

		uint64_t HeadSize = sizeof(head_t);

		SendQueueItem sendBuffer;
		head_t headBuffer = {};
		int64_t sendHeadCnt = 0;
		int64_t sendCnt = 0;

		std::vector<uint8_t> recvBuffer;
		int64_t recvHeadCnt = 0;
		int64_t recvCnt = 0;

	public:
		TCPConnectionProcessor() = default;
		TCPConnectionProcessor(
			std::unique_ptr<TCPStream>&& stream,
			std::function<std::optional<SendQueueItem>(NetEndpoint)> getSendData,
			std::function<void(std::vector<uint8_t>&&, NetEndpoint)> pullRecvData,
			std::function<void(NetEndpoint)> streamError
		) : stream(std::move(stream)), getSendData(std::move(getSendData)), pullRecvData(std::move(pullRecvData)), streamError(std::move(streamError)) {}
		void update();
		TCPStreamStatus status() { return stream->status(); }
		TCPStreamStatus connect(NetEndpoint remote) { return stream->connect(std::move(remote)); }
		NetEndpoint local() const { return stream->local(); }
		NetEndpoint remote() const { return stream->remote(); }
	};

	class TCPClient
	{
	private:
		TCPConnectionProcessor processor;

		
		std::deque<std::vector<uint8_t>> recvQueue;
		std::unique_ptr<std::mutex> recvMutex = std::make_unique<std::mutex>();

		
		std::deque<SendQueueItem> sendQueue;
		std::unique_ptr<std::mutex> sendMutex = std::make_unique<std::mutex>();

	public:
		constexpr static int64_t HeadSize = 4;
		TCPClient(NetEndpoint remote, NetEndpoint local = {}) : 
			processor(std::make_unique<TCPStream>(std::move(remote), std::move(local)), 
				[this](NetEndpoint) -> std::optional<SendQueueItem>
				{
					std::lock_guard lock{ *sendMutex };
					if (sendQueue.empty()) return std::nullopt;
					auto tmp = std::move(sendQueue.front());
					sendQueue.pop_front();
					return std::make_optional<SendQueueItem>(std::move(tmp));
				},
				[this](std::vector<uint8_t>&& data, NetEndpoint)
				{
					std::lock_guard lock{ *recvMutex };
					recvQueue.emplace_back(std::move(data));
				},
				[this](NetEndpoint)
				{
					std::lock_guard lock{ *sendMutex };
					for (auto& item : sendQueue)
					{
						item.status->store(AsyncSendStatus::Failed);
					}
					sendQueue.clear();
				}
			){}
		TCPClient(AddressFamily family) : TCPClient({}, NetEndpoint{ family == AddressFamily::IPv4 ? "0.0.0.0" : "::", 0, family }) {};
		TCPStreamStatus status() { return processor.status(); }
		bool valid() { return status() != TCPStreamStatus::Error; }
		TCPStreamStatus connect(NetEndpoint remote) { return processor.connect(remote); }
		void update();
		NetEndpoint local() const { return processor.local(); }
		NetEndpoint remote() const { return processor.remote(); }
		AsyncSendResult asyncSend(std::vector<uint8_t> data);
		AsyncSendResult asyncSendRef(std::span<const uint8_t> data);
		std::optional<std::vector<uint8_t>> getPackage();
		~TCPClient();
	};

	struct NetPackage
	{
		NetEndpoint remote;
		std::vector<uint8_t> data;
	};

	class TCPServer
	{
	private:
		struct SendItem
		{
			SendQueueItem data;
			NetEndpoint remote;
		};
		struct Client
		{
			TCPConnectionProcessor connection;
			std::deque<SendQueueItem> sendQueue;
		};

		TCPListener listener;

		std::deque<NetPackage> recvQueue;
		std::unique_ptr<std::mutex> recvMutex = std::make_unique<std::mutex>();

		std::deque<SendItem> sendQueue;
		std::unique_ptr<std::mutex> sendMutex = std::make_unique<std::mutex>();

		std::unordered_map<NetEndpoint, Client> clients;
		std::unique_ptr<std::mutex> clientsMutex = std::make_unique<std::mutex>();

		std::vector<NetEndpoint> errorClients;
		std::unique_ptr<std::mutex> errorMutex = std::make_unique<std::mutex>();

	public:
		TCPServer(NetEndpoint local) : listener(local) {}
		TCPServer(uint16_t port) : listener(port) {}
		void update();
		std::vector<std::function<void()>> updateTasks();
		NetEndpoint local() const { return listener.local(); }
		std::vector<NetEndpoint> remote() const;
		AsyncSendResult asyncSend(std::vector<uint8_t> data, NetEndpoint remote);
		AsyncSendResult asyncSendRef(std::span<const uint8_t> data, NetEndpoint remote);
		std::optional<NetPackage> getPackage();
		TCPListenerStatus status() { return listener.status(); }
		std::optional<TCPStreamStatus> remoteStatus(NetEndpoint remote);
		bool valid() { return status() != TCPListenerStatus::Error; }
		~TCPServer();
	};
}