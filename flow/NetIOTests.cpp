/*
 * NetIOTests.cpp
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

#include "NetIO.h"

#include <array>
#include <limits>
#include <utility>
#include <vector>

#include <boost/asio/socket_base.hpp>

#include "flow/CoroUtils.h"
#include "flow/IConnection.h"
#include "flow/Knobs.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.h"
#include "flow/UnitTest.h"
#include "flow/genericactors.h"
#include "flow/network.h"
#include "flow/serialize.h"

void forceLinkNetIOTests() {}

namespace {

using N2::NetIODirection;
using N2::NetIOThreads;

NetIOThreads::Counters counters() {
	return N2::getNetIOThreads().counters();
}

FlowKnobs& flowKnobs() {
	return const_cast<FlowKnobs&>(*FLOW_KNOBS);
}

template <class T>
class ScopedKnob : NonCopyable {
public:
	ScopedKnob(T& knob, T value) : knob(knob), saved(std::exchange(knob, value)) {}
	~ScopedKnob() { knob = saved; }

private:
	T& knob;
	const T saved;
};

struct Mode {
	const char* name;
	int threads;
	bool incoming;
	bool outgoing;
};

constexpr std::array<Mode, 4> allModes{ { { "off", 0, true, true },
	                                      { "incoming", 2, true, false },
	                                      { "outgoing", 2, false, true },
	                                      { "both", 2, true, true } } };

struct ScopedMode {
	ScopedKnob<int> threads;
	ScopedKnob<bool> incoming;
	ScopedKnob<bool> outgoing;

	explicit ScopedMode(const Mode& mode)
	  : threads(flowKnobs().NET_IO_OFFLOAD_THREADS, mode.threads),
	    incoming(flowKnobs().NET_IO_OFFLOAD_INCOMING, mode.incoming),
	    outgoing(flowKnobs().NET_IO_OFFLOAD_OUTGOING, mode.outgoing) {}
};

struct NoTLSThrottling {
	ScopedKnob<int> server{ flowKnobs().TLS_SERVER_CONNECTION_THROTTLE_ATTEMPTS, std::numeric_limits<int>::max() };
	ScopedKnob<int> client{ flowKnobs().TLS_CLIENT_CONNECTION_THROTTLE_ATTEMPTS, std::numeric_limits<int>::max() };
};

int tlsCases() {
	if (!g_network->getTLSConfig().getCertificatePathSync().empty()) {
		return 2;
	}
	TraceEvent(SevWarnAlways, "NetIOTestTLSSkipped").suppressFor(3600).detail("Reason", "No TLS certificate");
	return 1;
}

uint8_t patternByte(int64_t offset, uint32_t seed) {
	return static_cast<uint8_t>(((static_cast<uint64_t>(offset) + seed) * 0x9E3779B97F4A7C15ULL) >> 56);
}

class TestSendBuffer : public SendBuffer {
public:
	TestSendBuffer(uint8_t* data, int size) {
		_data = data;
		next = nullptr;
		bytes_written = size;
		bytes_sent = 0;
	}
};

Future<Void> sendPattern(Reference<IConnection> conn, int64_t total, uint32_t seed) {
	std::vector<uint8_t> chunk(size_t{ 256 } * 1024);
	int64_t offset = 0;
	try {
		while (offset < total) {
			const int size = static_cast<int>(std::min<int64_t>(
			    deterministicRandom()->randomInt(1, static_cast<int>(chunk.size()) + 1), total - offset));
			for (int i = 0; i < size; ++i) {
				chunk[i] = patternByte(offset + i, seed);
			}
			TestSendBuffer buffer(chunk.data(), size);
			while (true) {
				buffer.bytes_sent += conn->write(&buffer, FLOW_KNOBS->MAX_PACKET_SEND_BYTES);
				if (buffer.bytes_unsent() == 0) {
					break;
				}
				co_await conn->onWritable();
				co_await yield(TaskPriority::WriteSocket);
			}
			offset += size;
			if (deterministicRandom()->random01() < 0.05) {
				co_await yield();
			}
		}
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "NetIOTestSendError", conn->getDebugID())
		    .errorUnsuppressed(e)
		    .detail("Offset", offset)
		    .detail("Total", total);
		throw;
	}
}

Future<Void> receivePattern(Reference<IConnection> conn, int64_t total, uint32_t seed) {
	std::vector<uint8_t> buffer(size_t{ 64 } * 1024);
	int64_t offset = 0;
	try {
		while (offset < total) {
			const int want = static_cast<int>(std::min<int64_t>(
			    deterministicRandom()->randomInt(1, static_cast<int>(buffer.size()) + 1), total - offset));
			int n = conn->read(buffer.data(), buffer.data() + want);
			if (n == 0) {
				co_await conn->onReadable();
				co_await delay(0, TaskPriority::ReadSocket);
				continue;
			}
			for (int i = 0; i < n; ++i) {
				if (buffer[i] != patternByte(offset + i, seed)) {
					TraceEvent(SevError, "NetIOTestDataMismatch").detail("Offset", offset + i).detail("Seed", seed);
					ASSERT(false);
				}
			}
			offset += n;
		}
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "NetIOTestReceiveError", conn->getDebugID())
		    .errorUnsuppressed(e)
		    .detail("Offset", offset)
		    .detail("Total", total);
		throw;
	}
}

Future<Void> expectClosed(Reference<IConnection> conn) {
	std::vector<uint8_t> buffer(size_t{ 64 } * 1024);
	const double deadline = now() + 30.0;
	while (true) {
		Optional<Void> readable = Void();
		try {
			if (conn->read(buffer.data(), buffer.data() + buffer.size()) == 0) {
				readable = co_await timeout(conn->onReadable(), std::max(deadline - now(), 0.0));
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			ASSERT_EQ(e.code(), error_code_connection_failed);
			co_return;
		}
		if (!readable.present()) {
			TraceEvent(SevError, "NetIOTestCloseNotSeen", conn->getDebugID()).detail("Seconds", 30.0);
			ASSERT(false);
		}
	}
}

int setSendBuffer(Reference<IConnection> conn, int bytes) {
	boost::system::error_code error;
	if (bytes > 0 && conn->getSocket().set_option(boost::asio::socket_base::send_buffer_size(bytes), error)) {
		ASSERT(false);
	}
	boost::asio::socket_base::send_buffer_size effective;
	if (conn->getSocket().get_option(effective, error)) {
		ASSERT(false);
	}
	return effective.value();
}

struct Loopback {
	Reference<IConnection> server;
	Reference<IConnection> client;
};

Future<Loopback> connectLoopback(bool tls, int serverSendBuffer = 0, int clientSendBuffer = 0, bool external = false) {
	Reference<IListener> listener =
	    INetworkConnections::net()->listen(NetworkAddress::parse(tls ? "127.0.0.1:0:tls" : "127.0.0.1:0"));
	Future<Reference<IConnection>> accepted = listener->accept();
	Loopback result;
	result.client = co_await (external ? INetworkConnections::net()->connectExternal(listener->getListenAddress())
	                                   : INetworkConnections::net()->connect(listener->getListenAddress()));
	result.server = co_await accepted;
	if (serverSendBuffer > 0) {
		setSendBuffer(result.server, serverSendBuffer);
	}
	if (clientSendBuffer > 0) {
		setSendBuffer(result.client, clientSendBuffer);
	}
	co_await timeoutError(result.server->acceptHandshake() && result.client->connectHandshake(), 30.0);
	co_return result;
}

Future<Void> transferBothWays(Loopback c, int64_t bytes, uint32_t seed) {
	co_await timeoutError(sendPattern(c.client, bytes, seed) && receivePattern(c.server, bytes, seed) &&
	                          sendPattern(c.server, bytes, seed + 1) && receivePattern(c.client, bytes, seed + 1),
	                      300.0);
}

Future<Void> closeAndExpectFailure(Reference<IConnection> closer, Reference<IConnection> survivor) {
	closer->close();
	co_await expectClosed(survivor);
	survivor->close();
}

struct Baseline {
	NetIOThreads::Counters start = counters();

	void checkOffloaded(int64_t expectIncoming, int64_t expectOutgoing) const {
		const NetIOThreads::Counters current = counters();
		ASSERT_EQ(current.offloadedIncoming - start.offloadedIncoming, expectIncoming);
		ASSERT_EQ(current.offloadedOutgoing - start.offloadedOutgoing, expectOutgoing);
	}
};

Future<Void> waitForConnectionsReleased(Baseline baseline) {
	const double deadline = now() + 30.0;
	while (counters().socketsOpen > baseline.start.socketsOpen ||
	       counters().channelsAlive > baseline.start.channelsAlive ||
	       counters().bufferedBytes != baseline.start.bufferedBytes) {
		if (now() > deadline) {
			TraceEvent(SevError, "NetIOTestConnectionsNotReleased")
			    .detail("SocketsOpen", counters().socketsOpen)
			    .detail("ChannelsAlive", counters().channelsAlive)
			    .detail("BufferedBytes", counters().bufferedBytes)
			    .detail("SocketsBaseline", baseline.start.socketsOpen)
			    .detail("ChannelsBaseline", baseline.start.channelsAlive)
			    .detail("BufferedBytesBaseline", baseline.start.bufferedBytes);
			ASSERT(false);
		}
		co_await delay(0.01);
	}
}

} // namespace

TEST_CASE("noSim/flow/Net2/NetIO/OffPathStartsNoThreads") {
	noUnseed = true;
	const int64_t threadsBefore = counters().threadsStarted;
	const Baseline before;
	ScopedMode off(allModes[0]);
	for (int tls = 0; tls < tlsCases(); ++tls) {
		Loopback c = co_await connectLoopback(tls != 0);
		co_await transferBothWays(c, 4 << 20, 7);
		co_await closeAndExpectFailure(c.client, c.server);
	}
	before.checkOffloaded(0, 0);
	ASSERT_EQ(counters().threadsStarted, threadsBefore);
	if (threadsBefore == 0) {
		ASSERT_EQ(g_network->getNetIOThreadsCPUSeconds(), 0.0);
	}
}

TEST_CASE("noSim/flow/Net2/NetIO/Loopback") {
	noUnseed = true;
	const int iterations = static_cast<int>(params.getInt("iterations").orDefault(1));
	const int64_t bytes = params.getInt("bytes").orDefault(16 << 20);
	const Baseline baseline;
	for (int iteration = 0; iteration < iterations; ++iteration) {
		for (int tls = 0; tls < tlsCases(); ++tls) {
			for (const Mode& mode : allModes) {
				ScopedMode knobs(mode);
				const Baseline before;
				Loopback c = co_await connectLoopback(tls != 0);
				before.checkOffloaded(mode.threads > 0 && mode.incoming, mode.threads > 0 && mode.outgoing);
				co_await transferBothWays(c, bytes, iteration * 16 + tls * 4);
				if (deterministicRandom()->coinflip()) {
					co_await closeAndExpectFailure(c.client, c.server);
				} else {
					co_await closeAndExpectFailure(c.server, c.client);
				}
				TraceEvent("NetIOTestLoopbackDone")
				    .detail("Mode", mode.name)
				    .detail("TLS", tls)
				    .detail("Iteration", iteration);
			}
		}
	}
	co_await waitForConnectionsReleased(baseline);

	{
		ScopedMode knobs(allModes[3]);
		const Baseline before;
		Loopback c = co_await connectLoopback(false, 0, 0, true);
		before.checkOffloaded(1, 0);
		co_await transferBothWays(c, 1 << 20, 99);
		c.client->close();
		c.server->close();
	}
	co_await waitForConnectionsReleased(baseline);
}

TEST_CASE("noSim/flow/Net2/NetIO/SmallSocketBuffers") {
	noUnseed = true;
	const int iterations = static_cast<int>(params.getInt("iterations").orDefault(1));
	const int64_t bytes = params.getInt("bytes").orDefault(8 << 20);
	const Baseline baseline;
	constexpr int smallSendBuffer = 4096;
	constexpr int fullTLSRecord = 16 * 1024;
	ScopedKnob bufferBytes(flowKnobs().NET_IO_OFFLOAD_BUFFER_BYTES, 64 * 1024);
	for (int iteration = 0; iteration < iterations; ++iteration) {
		for (int tls = 0; tls < tlsCases(); ++tls) {
			for (const Mode& mode : allModes) {
				if (tls != 0 && mode.threads == 0) {
					continue;
				}
				ScopedMode knobs(mode);
				const bool smallServer = tls == 0 || (mode.threads > 0 && mode.incoming);
				const bool smallClient = tls == 0 || (mode.threads > 0 && mode.outgoing);
				Loopback c = co_await connectLoopback(
				    tls != 0, smallServer ? smallSendBuffer : 0, smallClient ? smallSendBuffer : 0);
				TraceEvent("NetIOTestSmallBuffersStart")
				    .detail("Mode", mode.name)
				    .detail("TLS", tls)
				    .detail("Iteration", iteration);
				co_await transferBothWays(c, bytes, 1000 + iteration * 16 + tls * 4);
				co_await closeAndExpectFailure(c.client, c.server);
			}
		}
	}
	{
		ScopedMode off(allModes[0]);
		Loopback c = co_await connectLoopback(false);
		ASSERT_LT(setSendBuffer(c.client, smallSendBuffer), fullTLSRecord);
		c.client->close();
		c.server->close();
	}
	co_await waitForConnectionsReleased(baseline);
}

TEST_CASE("noSim/flow/Net2/NetIO/CloseRaces") {
	noUnseed = true;
	const int iterations = static_cast<int>(params.getInt("iterations").orDefault(200));
	const bool cancelHandshakes = params.getInt("cancelHandshakes").orDefault(0) != 0;
	const Baseline baseline;
	NoTLSThrottling noThrottling;
	for (int iteration = 0; iteration < iterations; ++iteration) {
		const bool tls = tlsCases() == 2 && deterministicRandom()->coinflip();
		ScopedMode knobs(allModes[deterministicRandom()->randomInt(1, static_cast<int>(allModes.size()))]);
		ScopedKnob bufferBytes(flowKnobs().NET_IO_OFFLOAD_BUFFER_BYTES, deterministicRandom()->randomInt(1, 4) * 32768);
		const int action = deterministicRandom()->randomInt(0, cancelHandshakes ? 5 : 4);

		if (action == 4) {
			Reference<IListener> listener =
			    INetworkConnections::net()->listen(NetworkAddress::parse(tls ? "127.0.0.1:0:tls" : "127.0.0.1:0"));
			Future<Reference<IConnection>> accepted = listener->accept();
			Reference<IConnection> client = co_await INetworkConnections::net()->connect(listener->getListenAddress());
			Reference<IConnection> server = co_await accepted;
			Future<Void> serverHandshake = server->acceptHandshake();
			Future<Void> clientHandshake = client->connectHandshake();
			co_await delay(deterministicRandom()->random01() * 0.002);
			clientHandshake = Future<Void>();
			client->close();
			bool handshook = true;
			try {
				co_await timeoutError(serverHandshake, 30.0);
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw;
				}
				ASSERT_EQ(e.code(), error_code_connection_failed);
				handshook = false;
			}
			if (handshook) {
				co_await expectClosed(server);
			}
			server->close();
			continue;
		}

		Loopback c = co_await connectLoopback(tls);
		if (action == 3) {
			c.server->close();
			c.client->close();
			continue;
		}
		const int64_t bytes = deterministicRandom()->randomInt(1, 8 << 20);
		std::vector<Future<Void>> transfer{ sendPattern(c.client, bytes, iteration),
			                                receivePattern(c.server, bytes, iteration),
			                                sendPattern(c.server, bytes, iteration + 1),
			                                receivePattern(c.client, bytes, iteration + 1) };
		co_await delay(deterministicRandom()->random01() * 0.005);
		if (action == 2) {
			transfer.clear();
			c = Loopback();
			continue;
		}
		transfer.clear();
		if (action == 0) {
			co_await closeAndExpectFailure(c.client, c.server);
		} else {
			co_await closeAndExpectFailure(c.server, c.client);
		}
	}
	co_await waitForConnectionsReleased(baseline);
}

namespace {

Future<Void> runPair(Loopback c, int64_t bytes, uint32_t seed, int action) {
	if (action == 0) {
		co_await transferBothWays(c, bytes, seed);
		co_await closeAndExpectFailure(c.client, c.server);
		co_return;
	}
	std::vector<Future<Void>> transfer{ sendPattern(c.client, bytes, seed),
		                                receivePattern(c.server, bytes, seed),
		                                sendPattern(c.server, bytes, seed + 1),
		                                receivePattern(c.client, bytes, seed + 1) };
	co_await delay(deterministicRandom()->random01() * 0.05);
	transfer.clear();
	if (action == 1) {
		co_await closeAndExpectFailure(c.client, c.server);
	} else if (action == 2) {
		co_await closeAndExpectFailure(c.server, c.client);
	}
}

} // namespace

TEST_CASE("noSim/flow/Net2/NetIO/ManyConnections") {
	noUnseed = true;
	const int iterations = static_cast<int>(params.getInt("iterations").orDefault(1));
	const int pairs = static_cast<int>(params.getInt("pairs").orDefault(64));
	const Baseline baseline;
	NoTLSThrottling noThrottling;
	ScopedMode knobs(allModes[3]);
	for (int iteration = 0; iteration < iterations; ++iteration) {
		const Baseline before;
		std::vector<Loopback> connections;
		connections.reserve(pairs);
		for (int i = 0; i < pairs; ++i) {
			connections.push_back(co_await connectLoopback(tlsCases() == 2 && i % 2 == 1));
		}
		std::vector<Future<Void>> runs;
		runs.reserve(pairs);
		for (int i = 0; i < pairs; ++i) {
			runs.push_back(runPair(std::move(connections[i]),
			                       deterministicRandom()->randomInt(1, 2 << 20),
			                       iteration * pairs * 2 + i * 2,
			                       (i / 2) % 4));
		}
		connections.clear();
		co_await timeoutError(waitForAll(runs), 300.0);
		runs.clear();
		before.checkOffloaded(pairs, pairs);
		TraceEvent("NetIOTestManyConnectionsDone").detail("Pairs", pairs).detail("Iteration", iteration);
	}
	co_await waitForConnectionsReleased(baseline);
}

namespace {

Future<Void> sampleBufferedBytes(int64_t* max) {
	while (true) {
		*max = std::max(*max, counters().bufferedBytes);
		co_await delay(0.001);
	}
}

} // namespace

TEST_CASE("noSim/flow/Net2/NetIO/ProcessBufferLimit") {
	noUnseed = true;
	const int pairs = 8;
	const int64_t limit = int64_t{ 128 } * 1024;
	const Baseline baseline;
	ScopedMode knobs(allModes[3]);
	ScopedKnob processBytes(flowKnobs().NET_IO_OFFLOAD_PROCESS_BUFFER_BYTES, limit);
	const int64_t stopsBefore = counters().bufferLimitStops;
	std::vector<Loopback> connections;
	std::vector<Future<Void>> transfers;
	for (int i = 0; i < pairs; ++i) {
		connections.push_back(co_await connectLoopback(false));
		transfers.push_back(transferBothWays(connections.back(), 4 << 20, 5000 + i * 2));
	}
	int64_t maxBuffered = 0;
	Future<Void> sampler = sampleBufferedBytes(&maxBuffered);
	co_await waitForAll(transfers);
	sampler = Future<Void>();
	transfers.clear();
	const int64_t stops = counters().bufferLimitStops - stopsBefore;
	TraceEvent("NetIOTestProcessBufferLimit")
	    .detail("MaxBufferedBytes", maxBuffered)
	    .detail("Limit", limit)
	    .detail("Stops", stops);
	const int64_t channelDirections = int64_t{ pairs } * 2 * 2;
	ASSERT_LE(maxBuffered, limit + (channelDirections + 1) * 16 * 1024);
	ASSERT_GT(stops, 0);
	for (const Loopback& c : connections) {
		c.client->close();
		c.server->close();
	}
	connections.clear();
	co_await waitForConnectionsReleased(baseline);
}

TEST_CASE("noSim/flow/Net2/NetIO/ThreadCPUAccounting") {
	noUnseed = true;
	ScopedMode knobs(allModes[3]);
	Loopback c = co_await connectLoopback(tlsCases() == 2);
	const double processBefore = getProcessorTimeProcess();
	const double before = g_network->getNetIOThreadsCPUSeconds();
	co_await transferBothWays(c, 64 << 20, 77);
	const double after = g_network->getNetIOThreadsCPUSeconds();
	const double processAfter = getProcessorTimeProcess();
	TraceEvent("NetIOTestThreadCPU")
	    .detail("ProcessCPUSeconds", processAfter - processBefore)
	    .detail("NetIOThreadsCPUSeconds", after - before);
#if defined(__linux__) || defined(__FreeBSD__) || defined(__APPLE__)
	ASSERT_GT(after, before);
#endif
	ASSERT_LE(after - before, processAfter - processBefore);
	co_await closeAndExpectFailure(c.client, c.server);
}

TEST_CASE("noSim/flow/Net2/NetIO/InvalidKnobs") {
	noUnseed = true;
	ScopedMode knobs(allModes[3]);
	ScopedKnob bufferBytes(flowKnobs().NET_IO_OFFLOAD_BUFFER_BYTES, 0);
	const Baseline before;
	Loopback c = co_await connectLoopback(false);
	before.checkOffloaded(0, 0);
	co_await transferBothWays(c, 1 << 20, 88);
	co_await closeAndExpectFailure(c.client, c.server);
}

TEST_CASE("noSim/flow/Net2/NetIO/StopJoinsThreads") {
	noUnseed = true;
	ScopedMode knobs(allModes[3]);
	boost::asio::io_context unused;
	N2::NetIOThreads threads(unused);
	ASSERT_EQ(threads.counters().threadsStarted, 0);
	ASSERT(threads.pick(NetIODirection::Incoming) != nullptr);
	ASSERT_EQ(threads.counters().threadsStarted, FLOW_KNOBS->NET_IO_OFFLOAD_THREADS);
	threads.stop();
	ASSERT(threads.pick(NetIODirection::Outgoing) == nullptr);
	ASSERT_GE(threads.cpuSeconds(), 0.0);
	threads.stop();
	return Void();
}
