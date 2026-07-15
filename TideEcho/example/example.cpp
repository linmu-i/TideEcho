#include <iostream>
#include <stdlib.h>
#include <thread>

#include <TideEcho.h>

std::atomic<bool> running = true;

int main()
{
	tideecho::Initialize();
	int choice = 0;
	std::cout << "1. Client" << std::endl;
	std::cout << "2. Server" << std::endl;
	std::cout << "Input choice: ";
	std::cin >> choice;
	if (choice == 1)
	{
		std::string ip;
		std::cout << "Input IP: ";
		std::cin >> ip;
		uint16_t port;
		std::cout << "Input port: ";
		std::cin >> port;
		auto remote = tideecho::NetEndpoint{ ip, port, tideecho::AddressFamily::IPv4 };
		tideecho::TCPClient client{ remote };

		std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // 等待连接建立
		tideecho::TCPStreamStatus status = client.status();
		if (status != tideecho::TCPStreamStatus::Connected)
		{
			std::cerr << "Failed to connect to server." << std::endl;
			return 1;
		}

		auto task = [&client]()
			{
				while (running && client.status() != tideecho::TCPStreamStatus::Error)
				{
					client.update();
				}
				running = false;
			};
		std::thread t(task);

		while (running)
		{
			std::string input;
			std::getline(std::cin, input);
			if (input == "exit")
			{
				running = false;
				break;
			}
			else
			{
				client.asyncSend(std::vector<uint8_t>(input.begin(), input.end()));
			}
			auto pkgOpt = client.getPackage();
			while (pkgOpt != std::nullopt)
			{
				auto& pkg = *pkgOpt;
				std::string data(pkg.begin(), pkg.end());
				std::cout << "Received from " << client.remote().toString() << ": " << data << std::endl;
				pkgOpt = client.getPackage();
			}
		}
		t.join();
	}
	else if (choice == 2)
	{
		uint16_t port;
		std::cout << "Input port: ";
		std::cin >> port;
		tideecho::TCPServer server{ static_cast<uint16_t>(port) };

		auto task = [&server]()
			{
				while (running)
				{
					server.update();
				}
			};

		std::thread t(task);

		while (running)
		{
			auto pkgOpt = server.getPackage();
			while (pkgOpt != std::nullopt)
			{
				auto& pkg = *pkgOpt;
				std::string data(pkg.data.begin(), pkg.data.end());
				std::cout << "Received from " << pkg.remote.toString() << ": " << data << std::endl;
				pkgOpt = server.getPackage();
			}
			std::string input;
			std::getline(std::cin, input);
			if (input == "exit")
			{
				running = false;
				break;
			}
			else
			{
				for (const auto& remote : server.remote())
				{
					server.asyncSend(std::vector<uint8_t>(input.begin(), input.end()), remote);
				}
			}
		}

		t.join();
	}
	else
	{
		std::cerr << "Invalid choice." << std::endl;
		return 1;
	}
	tideecho::Cleanup();
	return 0;
}