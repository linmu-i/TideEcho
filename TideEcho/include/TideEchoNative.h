#pragma once

#include <cstdint>
#include <variant>
#include <optional>
#include <cstring>

#ifdef _WIN32
//Win32
#include <WinSock2.h>
#include <WS2tcpip.h>
#include <iphlpapi.h>
#include <mstcpip.h>

namespace tideecho
{
	inline SOCKET I64ToSocket(int64_t s)
	{
		SOCKET r = 0;
		memcpy(&r, &s, sizeof(SOCKET));
		return r;
	}

	inline int64_t SocketToI64(SOCKET s)
	{
#ifndef _WIN64
		int32_t tmp = 0;
		memcpy(&tmp, &s, sizeof(int32_t));
		int64_t r = tmp;
		return r;
#else
		int64_t r = 0;
		memcpy(&r, &s, sizeof(int64_t));
		return r;
#endif
	}

	inline std::optional<std::variant<sockaddr_in, sockaddr_in6>> EndpointToSockaddr(const NetEndpoint& endpoint)
	{
		if (endpoint.addrFamily() == AddressFamily::IPv4)
		{
			sockaddr_in addr = {};
			addr.sin_family = AF_INET;
			inet_pton(AF_INET, endpoint.ip().c_str(), &addr.sin_addr);
			addr.sin_port = htons(endpoint.port());
			return addr;
		}
		else if (endpoint.addrFamily() == AddressFamily::IPv6)
		{
			sockaddr_in6 addr = {};
			addr.sin6_family = AF_INET6;
			inet_pton(AF_INET6, endpoint.ip().c_str(), &addr.sin6_addr);
			addr.sin6_port = htons(endpoint.port());
			return addr;
		}
		return std::nullopt;
	}
}

#else
//POSIX
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace tideecho
{
	inline int I64ToSocket(int64_t s)
	{
		return static_cast<int>(s);
	}

	inline int64_t SocketToI64(int s)
	{
		return static_cast<int64_t>(s);
	}

	inline std::optional<std::variant<sockaddr_in, sockaddr_in6>> EndpointToSockaddr(const NetEndpoint& endpoint)
	{
		if (endpoint.addrFamily() == AddressFamily::IPv4)
		{
			sockaddr_in addr = {};
			addr.sin_family = AF_INET;
			inet_pton(AF_INET, endpoint.ip().c_str(), &addr.sin_addr);
			addr.sin_port = htons(endpoint.port());
			return addr;
		}
		else if (endpoint.addrFamily() == AddressFamily::IPv6)
		{
			sockaddr_in6 addr = {};
			addr.sin6_family = AF_INET6;
			inet_pton(AF_INET6, endpoint.ip().c_str(), &addr.sin6_addr);
			addr.sin6_port = htons(endpoint.port());
			return addr;
		}
		return std::nullopt;
	}
}

#endif