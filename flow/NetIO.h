/*
 * NetIO.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FLOW_NETIO_H
#define FLOW_NETIO_H
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include <boost/asio/buffer.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/system/error_code.hpp>

#include "flow/flow.h"
#include "flow/NetworkAddress.h"

class SendBuffer;

// Net I/O offload (FLOW_KNOBS->NET_IO_OFFLOAD_*; Net2 only, never used in simulation). Once an eligible connection's
// handshake completes, its socket (and TLS stream) moves to a net I/O thread, which does all of its socket and TLS
// record I/O. The network thread only copies bytes to and from the connection's NetIOChannel.
//
// Threading: only the network thread touches Flow objects (Promise, Future, Reference, Arena). A net I/O thread
// touches only its own sockets and TLS streams, byte buffers, std:: locks and atomics, and NetIOChannel shared_ptrs.
namespace N2 {

class NetIOChannel;
class NetIOThread;

enum class NetIODirection : uint8_t { None, Incoming, Outgoing };

// Owned and driven by one net I/O thread: at most one read and one write in flight; handlers run on that thread.
class INetIOTransport {
public:
	using Handler = std::function<void(const boost::system::error_code&, size_t)>;

	virtual ~INetIOTransport() = default;

	virtual void asyncReadSome(boost::asio::mutable_buffer buffer, Handler handler) = 0;

	// The chain must stay unchanged until the handler runs.
	virtual void asyncWriteSome(SendBuffer const* data, int limit, Handler handler) = 0;

	virtual boost::system::error_code close() = 0;
};

std::unique_ptr<INetIOTransport> makeTcpNetIOTransport(boost::asio::ip::tcp::socket&& socket);
// Refuses renegotiation, so no handshake or certificate verification ever runs on a net I/O thread.
std::unique_ptr<INetIOTransport> makeTlsNetIOTransport(boost::asio::ssl::stream<boost::asio::ip::tcp::socket>&& stream,
                                                       boost::asio::ip::tcp::socket&& socket);

// Network thread. `socket` must be connected with no asynchronous operation pending. Returns it as a non-blocking
// socket of `thread`'s io_context, leaving `socket` closed, or nothing if it cannot be moved (`socket` may then be
// closed too).
std::optional<boost::asio::ip::tcp::socket> moveSocketToNetIOThread(boost::asio::ip::tcp::socket& socket,
                                                                    NetIOThread& thread);

// Bytes buffered by all channels against FLOW_KNOBS->NET_IO_OFFLOAD_PROCESS_BUFFER_BYTES. Relaxed atomics: the limit
// is soft.
class NetIOBudget : NonCopyable {
public:
	void setLimit(int64_t bytes) { limit.store(bytes, std::memory_order_relaxed); }
	int64_t left() const { return limit.load(std::memory_order_relaxed) - used.load(std::memory_order_relaxed); }
	void add(size_t bytes) { used.fetch_add(static_cast<int64_t>(bytes), std::memory_order_relaxed); }
	void remove(size_t bytes) { used.fetch_sub(static_cast<int64_t>(bytes), std::memory_order_relaxed); }
	void countStop() { stops.fetch_add(1, std::memory_order_relaxed); }

	int64_t usedBytes() const { return used.load(std::memory_order_relaxed); }
	int64_t stopCount() const { return stops.load(std::memory_order_relaxed); }

private:
	std::atomic<int64_t> limit{ 0 };
	std::atomic<int64_t> used{ 0 };
	std::atomic<int64_t> stops{ 0 };
};

// Network thread only. The threads start with the first offloaded connection (FLOW_KNOBS->NET_IO_OFFLOAD_THREADS at
// that time) and stop when Net2's run loop ends.
class NetIOThreads : NonCopyable {
public:
	struct Counters {
		int64_t threadsStarted = 0;
		int64_t offloadedIncoming = 0;
		int64_t offloadedOutgoing = 0;
		int64_t socketsOpen = 0;
		int64_t channelsAlive = 0;
		int64_t bufferedBytes = 0;
		int64_t bufferLimitStops = 0;
	};

	explicit NetIOThreads(boost::asio::io_context& networkContext);
	~NetIOThreads();

	// The thread for a connection that just completed its handshake, or nullptr if the connection stays on the network
	// thread.
	NetIOThread* pick(NetIODirection direction);

	// Stops and joins the threads; nothing is offloaded afterwards.
	void stop();

	double cpuSeconds() const;

	Counters counters() const;

private:
	boost::asio::io_context& networkContext;
	NetIOBudget budget;
	std::vector<std::unique_ptr<NetIOThread>> threads;
	size_t next = 0;
	bool disabled = false;
	Future<Void> metricsLogger;
};

// The network thread's side of an offloaded connection. Network thread only; destroying it closes the connection.
class NetIOConnection : NonCopyable {
public:
	NetIOConnection(NetIOThread& thread,
	                std::unique_ptr<INetIOTransport> transport,
	                NetIODirection direction,
	                UID id,
	                NetworkAddress peerAddress);
	~NetIOConnection();

	// Like read_some() and write_some() on a non-blocking socket. Once the connection is closed, read() returns the
	// bytes received before and then the error that closed it (bad_descriptor after close()); write() returns the error
	// right away.
	size_t read(uint8_t* begin, uint8_t* end, boost::system::error_code& error);
	size_t write(SendBuffer const* data, int limit, boost::system::error_code& error);

	// Ready when read() or write() would not return would_block.
	Future<Void> onReadable();
	Future<Void> onWritable();

	// Drops the bytes write() accepted that the I/O thread has not handed to the socket yet.
	void close();

private:
	friend class NetIOChannel;

	std::shared_ptr<NetIOChannel> channel;
	const UID id;
	const NetworkAddress peerAddress;
	Optional<Promise<Void>> readable;
	Optional<Promise<Void>> writable;

	void onEvents(uint8_t events);
};

NetIOThreads& getNetIOThreads();

} // namespace N2

#endif
