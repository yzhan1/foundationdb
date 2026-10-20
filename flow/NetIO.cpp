/*
 * NetIO.cpp
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

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>
#include <boost/range/iterator_range.hpp>
#include <openssl/ssl.h>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <unistd.h>
#endif

#include "flow/Knobs.h"
#include "flow/Platform.h"
#include "flow/SendBufferIterator.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/serialize.h"

#ifndef SSL_OP_NO_RENEGOTIATION
#error "Net I/O offload needs SSL_OP_NO_RENEGOTIATION (OpenSSL 1.1.0h or later)"
#endif

namespace N2 {

using boost::asio::ip::tcp;
using boost::system::error_code;

namespace {

constexpr size_t readChunkBytes = size_t{ 16 } * 1024;

class Chunk final : public SendBuffer {
public:
	static Chunk* make(size_t capacity) {
		void* memory = std::malloc(sizeof(Chunk) + capacity);
		if (memory == nullptr) {
			platform::outOfMemory();
		}
		auto* chunk = new (memory) Chunk();
		chunk->_data = reinterpret_cast<uint8_t*>(chunk + 1);
		chunk->next = nullptr;
		chunk->bytes_written = 0;
		chunk->bytes_sent = 0;
		return chunk;
	}

	static Chunk* copyOf(SendBuffer const* chain, int bytes) {
		Chunk* chunk = make(bytes);
		for (SendBuffer const* p = chain; p != nullptr && chunk->bytes_written < bytes; p = p->next) {
			const int n = std::min(p->bytes_unsent(), bytes - chunk->bytes_written);
			memcpy(chunk->_data + chunk->bytes_written, p->data() + p->bytes_sent, n);
			chunk->bytes_written += n;
		}
		return chunk;
	}

private:
	Chunk() = default;
};

class ChunkQueue : NonCopyable {
public:
	ChunkQueue() = default;
	~ChunkQueue() { clear(); }

	bool empty() const { return head == nullptr; }
	SendBuffer const* front() const { return head; }

	void push(Chunk* chunk) {
		if (tail != nullptr) {
			tail->next = chunk;
		} else {
			head = chunk;
		}
		tail = chunk;
	}

	void splice(ChunkQueue& other) {
		if (other.head == nullptr) {
			return;
		}
		if (tail != nullptr) {
			tail->next = other.head;
		} else {
			head = other.head;
		}
		tail = other.tail;
		other.head = other.tail = nullptr;
	}

	size_t copyOut(uint8_t* to, size_t size) {
		size_t copied = 0;
		while (head != nullptr && copied < size) {
			const size_t n = std::min(static_cast<size_t>(head->bytes_unsent()), size - copied);
			memcpy(to + copied, head->data() + head->bytes_sent, n);
			copied += n;
			consumeFront(n);
		}
		return copied;
	}

	void consume(size_t bytes) {
		while (bytes > 0) {
			const size_t n = std::min(bytes, static_cast<size_t>(head->bytes_unsent()));
			consumeFront(n);
			bytes -= n;
		}
	}

	void clear() {
		while (head != nullptr) {
			popFront();
		}
	}

private:
	SendBuffer* head = nullptr;
	SendBuffer* tail = nullptr;

	void consumeFront(size_t n) {
		head->bytes_sent += static_cast<int>(n);
		if (head->bytes_unsent() == 0) {
			popFront();
		}
	}

	void popFront() {
		SendBuffer* next = head->next;
		std::free(head);
		head = next;
		if (head == nullptr) {
			tail = nullptr;
		}
	}
};

struct BufferLimits {
	size_t connection;
	NetIOBudget& budget;
};

// One direction's bytes between its producer and consumer threads. Guarded by the channel's mutex; keeps
// limits.budget in step with its byte count.
class Pipe : NonCopyable {
public:
	explicit Pipe(const BufferLimits& limits) : limits(limits) {}
	~Pipe() { discard(); }

	ChunkQueue queued;
	bool producerWaits = false;
	bool consumerWaits = false;

	size_t size() const { return bytes; }

	size_t room() const {
		if (bytes >= limits.connection) {
			return 0;
		}
		const int64_t budgetLeft = limits.budget.left();
		if (budgetLeft <= 0) {
			return bytes == 0 ? std::min(limits.connection, readChunkBytes) : 0;
		}
		return std::min(limits.connection - bytes, static_cast<size_t>(budgetLeft));
	}

	void stopProducer() {
		if (!std::exchange(producerWaits, true) && bytes < limits.connection) {
			limits.budget.countStop();
		}
	}

	bool push(Chunk* chunk) {
		const size_t n = chunk->bytes_unsent();
		queued.push(chunk);
		bytes += n;
		limits.budget.add(n);
		return std::exchange(consumerWaits, false);
	}

	bool taken(size_t n) {
		bytes -= n;
		limits.budget.remove(n);
		if (producerWaits && (bytes == 0 || (bytes <= limits.connection / 2 && limits.budget.left() > 0))) {
			producerWaits = false;
			return true;
		}
		return false;
	}

	void discard() {
		queued.clear();
		limits.budget.remove(std::exchange(bytes, 0));
	}

private:
	const BufferLimits& limits;
	size_t bytes = 0;
};

class TcpTransport final : public INetIOTransport {
public:
	explicit TcpTransport(tcp::socket&& socket) : socket(std::move(socket)) {}

	void asyncReadSome(boost::asio::mutable_buffer buffer, Handler handler) override {
		socket.async_read_some(buffer, std::move(handler));
	}

	void asyncWriteSome(SendBuffer const* data, int limit, Handler handler) override {
		socket.async_write_some(
		    boost::iterator_range<SendBufferIterator>(SendBufferIterator(data, limit), SendBufferIterator()),
		    std::move(handler));
	}

	error_code close() override {
		error_code error;
		return socket.close(error);
	}

private:
	tcp::socket socket;
};

class TlsTransport final : public INetIOTransport {
public:
	using Stream = boost::asio::ssl::stream<tcp::socket>;

	TlsTransport(Stream&& stream, tcp::socket&& socket) : stream(std::move(stream)) {
		this->stream.next_layer() = std::move(socket);
		SSL_set_options(this->stream.native_handle(), SSL_OP_NO_RENEGOTIATION);
	}

	void asyncReadSome(boost::asio::mutable_buffer buffer, Handler handler) override {
		stream.async_read_some(buffer, boost::asio::bind_executor(stream.get_executor(), std::move(handler)));
	}

	void asyncWriteSome(SendBuffer const* data, int limit, Handler handler) override {
		stream.async_write_some(
		    boost::asio::const_buffer(data->data() + data->bytes_sent, std::min(data->bytes_unsent(), limit)),
		    boost::asio::bind_executor(stream.get_executor(), std::move(handler)));
	}

	error_code close() override {
		error_code error;
		return stream.next_layer().close(error);
	}

private:
	Stream stream;
};

} // namespace

std::unique_ptr<INetIOTransport> makeTcpNetIOTransport(tcp::socket&& socket) {
	return std::make_unique<TcpTransport>(std::move(socket));
}

std::unique_ptr<INetIOTransport> makeTlsNetIOTransport(boost::asio::ssl::stream<tcp::socket>&& stream,
                                                       tcp::socket&& socket) {
	return std::make_unique<TlsTransport>(std::move(stream), std::move(socket));
}

struct NetIOThreadStats {
	std::atomic<int64_t> offloadedIncoming{ 0 };
	std::atomic<int64_t> offloadedOutgoing{ 0 };
	std::atomic<int64_t> channels{ 0 };
	std::atomic<int64_t> connections{ 0 };
	std::atomic<int64_t> reads{ 0 };
	std::atomic<int64_t> bytesReceived{ 0 };
	std::atomic<int64_t> writes{ 0 };
	std::atomic<int64_t> bytesSent{ 0 };
	std::atomic<int64_t> eventBatches{ 0 };
};

class NetIOThread : NonCopyable {
public:
	NetIOThread(int index, boost::asio::io_context& networkContext, NetIOBudget& budget);
	~NetIOThread() { stop(); }

	boost::asio::io_context& context() { return ios; }
	int getIndex() const { return index; }
	NetIOThreadStats& getStats() { return stats; }
	NetIOBudget& getBudget() { return budget; }

	double cpuSeconds() const;

	void stop();

	void queueEvents(std::shared_ptr<NetIOChannel> channel);

private:
	const int index;
	NetIOBudget& budget;
	NetIOThreadStats stats;
	boost::asio::io_context ios;
	boost::asio::io_context& network;
	boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work;
	std::vector<std::shared_ptr<NetIOChannel>> eventBatch;
	THREAD_HANDLE handle;
	bool running = true;
	std::atomic<double> exitCPUSeconds{ 0 };

	void postEventBatch();
	static THREAD_FUNC_RETURN run(void* arg);
};

class NetIOChannel final : public std::enable_shared_from_this<NetIOChannel>, NonCopyable {
public:
	enum Event : uint8_t { Readable = 1, Writable = 2, Closed = 4 };

	NetIOChannel(NetIOThread& thread, std::unique_ptr<INetIOTransport> transport, NetIOConnection& owner)
	  : thread(thread), limits{ static_cast<size_t>(FLOW_KNOBS->NET_IO_OFFLOAD_BUFFER_BYTES), thread.getBudget() },
	    maxWriteBytes(FLOW_KNOBS->MAX_PACKET_SEND_BYTES), owner(&owner), transport(std::move(transport)) {
		tx.consumerWaits = true;
		++thread.getStats().channels;
		++thread.getStats().connections;
	}

	~NetIOChannel() {
		std::free(readChunk);
		--thread.getStats().channels;
	}

	void start() {
		post([](NetIOChannel& self) { self.startReading(); });
	}

	void detach() { owner = nullptr; }

	size_t read(uint8_t* begin, uint8_t* end, error_code& result) {
		bool resume = false;
		size_t n;
		{
			std::lock_guard<std::mutex> lock(mutex);
			n = rx.queued.copyOut(begin, end - begin);
			if (n == 0) {
				result = state == State::Open ? error_code(boost::asio::error::would_block) : error;
				return 0;
			}
			resume = rx.taken(n) && state == State::Open;
		}
		if (resume) {
			post([](NetIOChannel& self) { self.resumeReading(); });
		}
		return n;
	}

	size_t write(SendBuffer const* data, int limit, error_code& result) {
		size_t room;
		{
			std::lock_guard<std::mutex> lock(mutex);
			if (state != State::Open) {
				result = error;
				return 0;
			}
			room = std::min(tx.room(), static_cast<size_t>(std::max(limit, 0)));
		}
		size_t bytes = 0;
		for (SendBuffer const* p = data; p != nullptr && bytes < room; p = p->next) {
			bytes += p->bytes_unsent();
		}
		bytes = std::min(bytes, room);
		if (bytes == 0) {
			result = boost::asio::error::would_block;
			return 0;
		}
		Chunk* chunk = Chunk::copyOf(data, static_cast<int>(bytes));
		bool wakeWriter;
		{
			std::lock_guard<std::mutex> lock(mutex);
			if (state != State::Open) {
				std::free(chunk);
				result = error;
				return 0;
			}
			wakeWriter = tx.push(chunk);
		}
		if (wakeWriter) {
			post([](NetIOChannel& self) { self.resumeWriting(); });
		}
		return bytes;
	}

	bool readableOrWait() {
		std::lock_guard<std::mutex> lock(mutex);
		if (!rx.queued.empty() || state != State::Open) {
			return true;
		}
		rx.consumerWaits = true;
		return false;
	}

	bool writableOrWait() {
		std::lock_guard<std::mutex> lock(mutex);
		if (state != State::Open || tx.room() > 0) {
			return true;
		}
		tx.stopProducer();
		return false;
	}

	void close() {
		bool requestClose = false;
		{
			std::lock_guard<std::mutex> lock(mutex);
			rx.discard();
			if (state == State::Open) {
				beginClosing(boost::asio::error::bad_descriptor);
				requestClose = true;
			}
		}
		if (requestClose) {
			post([](NetIOChannel& self) { self.closeOnRequest(); });
		}
	}

	error_code getCloseError() {
		std::lock_guard<std::mutex> lock(mutex);
		return closeError;
	}

	void deliverEvents() {
		uint8_t pending;
		{
			std::lock_guard<std::mutex> lock(mutex);
			pending = std::exchange(events, 0);
		}
		if (pending != 0 && owner != nullptr) {
			owner->onEvents(pending);
		}
	}

private:
	enum class State { Open, Closing, Closed };

	NetIOThread& thread;
	const BufferLimits limits;
	const int maxWriteBytes;

	// Network thread only; nullptr once the owner is gone.
	NetIOConnection* owner;

	// I/O thread only.
	std::unique_ptr<INetIOTransport> transport;
	Chunk* readChunk = nullptr;
	ChunkQueue sending;
	bool reading = false;
	bool writing = false;

	std::mutex mutex;
	// Guarded by mutex.
	State state = State::Open;
	error_code error;
	error_code closeError;
	Pipe rx{ limits };
	Pipe tx{ limits };
	uint8_t events = 0; // non-zero exactly while the channel is queued in an event batch

	template <class F>
	void post(F f) {
		boost::asio::post(thread.context(), [self = shared_from_this(), f]() { f(*self); });
	}

	void beginClosing(const error_code& why) {
		state = State::Closing;
		error = why;
	}

	bool addEvents(uint8_t added) {
		const bool queue = events == 0;
		events |= added;
		return queue;
	}

	void startReading() {
		ASSERT(!reading);
		if (readChunk == nullptr) {
			readChunk = Chunk::make(readChunkBytes);
		}
		reading = true;
		transport->asyncReadSome(
		    boost::asio::mutable_buffer(readChunk->data(), readChunkBytes),
		    [self = shared_from_this()](const error_code& error, size_t bytes) { self->readDone(error, bytes); });
	}

	void resumeReading() {
		{
			std::lock_guard<std::mutex> lock(mutex);
			if (state != State::Open) {
				return;
			}
		}
		startReading();
	}

	void readDone(const error_code& readError, size_t bytes) {
		reading = false;
		NetIOThreadStats& stats = thread.getStats();
		++stats.reads;
		stats.bytesReceived += static_cast<int64_t>(bytes);
		Chunk* chunk = nullptr;
		if (bytes > 0) {
			if (bytes < readChunkBytes / 4) {
				chunk = Chunk::make(bytes);
				memcpy(chunk->data(), readChunk->data(), bytes);
			} else {
				chunk = std::exchange(readChunk, nullptr);
			}
			chunk->bytes_written = static_cast<int>(bytes);
		}
		bool notify = false;
		bool closeNow = false;
		bool readMore = false;
		bool open;
		{
			std::lock_guard<std::mutex> lock(mutex);
			if (state == State::Open) {
				if (chunk != nullptr) {
					notify = rx.push(std::exchange(chunk, nullptr)) && addEvents(Readable);
				}
				if (readError || bytes == 0) {
					beginClosing(readError ? readError : error_code(boost::asio::error::eof));
					closeNow = true;
				} else if (rx.room() == 0) {
					rx.stopProducer();
				} else {
					readMore = true;
				}
			}
			open = state == State::Open;
		}
		std::free(chunk);
		if (notify) {
			thread.queueEvents(shared_from_this());
		}
		if (closeNow) {
			closeSocket();
		}
		if (readMore) {
			startReading();
		} else if (!open) {
			finishIfIdle();
		}
	}

	void startWriting() {
		ASSERT(!writing);
		writing = true;
		transport->asyncWriteSome(
		    sending.front(), maxWriteBytes, [self = shared_from_this()](const error_code& error, size_t bytes) {
			    self->writeDone(error, bytes);
		    });
	}

	void resumeWriting() {
		{
			std::lock_guard<std::mutex> lock(mutex);
			if (state != State::Open) {
				return;
			}
			sending.splice(tx.queued);
			if (sending.empty()) {
				tx.consumerWaits = true;
				return;
			}
		}
		startWriting();
	}

	void writeDone(const error_code& writeError, size_t bytes) {
		writing = false;
		NetIOThreadStats& stats = thread.getStats();
		++stats.writes;
		stats.bytesSent += static_cast<int64_t>(bytes);
		sending.consume(bytes);
		bool notify = false;
		bool closeNow = false;
		bool writeMore = false;
		bool open;
		{
			std::lock_guard<std::mutex> lock(mutex);
			notify = tx.taken(bytes) && addEvents(Writable);
			if (state == State::Open) {
				if (writeError) {
					beginClosing(writeError);
					closeNow = true;
				} else {
					sending.splice(tx.queued);
					if (sending.empty()) {
						tx.consumerWaits = true;
					} else {
						writeMore = true;
					}
				}
			}
			open = state == State::Open;
		}
		if (notify) {
			thread.queueEvents(shared_from_this());
		}
		if (closeNow) {
			closeSocket();
		}
		if (writeMore) {
			startWriting();
		} else if (!open) {
			finishIfIdle();
		}
	}

	void closeOnRequest() {
		{
			std::lock_guard<std::mutex> lock(mutex);
			if (state != State::Closing) {
				return;
			}
		}
		closeSocket();
		finishIfIdle();
	}

	void closeSocket() {
		const error_code result = transport->close();
		if (result) {
			std::lock_guard<std::mutex> lock(mutex);
			closeError = result;
		}
	}

	void finishIfIdle() {
		if (reading || writing) {
			return;
		}
		{
			std::lock_guard<std::mutex> lock(mutex);
			if (state != State::Closing) {
				return;
			}
		}
		transport.reset();
		std::free(std::exchange(readChunk, nullptr));
		sending.clear();
		bool notify;
		{
			std::lock_guard<std::mutex> lock(mutex);
			state = State::Closed;
			tx.discard();
			notify = addEvents(Closed);
		}
		--thread.getStats().connections;
		if (notify) {
			thread.queueEvents(shared_from_this());
		}
	}
};

NetIOThread::NetIOThread(int index, boost::asio::io_context& networkContext, NetIOBudget& budget)
  : index(index), budget(budget), network(networkContext), work(boost::asio::make_work_guard(ios)) {
	std::string name = "fdb-netio-" + std::to_string(index);
	handle = startThread(run, this, 0, name.c_str());
}

double NetIOThread::cpuSeconds() const {
	return running ? getProcessorTimeThread(handle) : exitCPUSeconds.load();
}

void NetIOThread::stop() {
	if (!running) {
		return;
	}
	running = false;
	ios.stop();
	waitThread(handle);
}

void NetIOThread::queueEvents(std::shared_ptr<NetIOChannel> channel) {
	if (eventBatch.empty()) {
		boost::asio::post(ios, [this]() { postEventBatch(); });
	}
	eventBatch.push_back(std::move(channel));
}

void NetIOThread::postEventBatch() {
	++stats.eventBatches;
	boost::asio::post(network, [batch = std::move(eventBatch)]() {
		for (const auto& channel : batch) {
			channel->deliverEvents();
		}
	});
	eventBatch.clear();
}

THREAD_FUNC_RETURN NetIOThread::run(void* arg) {
	auto* self = static_cast<NetIOThread*>(arg);
	try {
		self->ios.run();
		self->exitCPUSeconds = getProcessorTimeThread();
	} catch (Error& e) {
		TraceEvent(SevError, "NetIOThreadError").error(e).detail("Thread", self->index);
		flushTraceFileVoid();
		crashAndDie();
	} catch (std::exception& e) {
		TraceEvent(SevError, "NetIOThreadError").detail("Thread", self->index).detail("What", e.what());
		flushTraceFileVoid();
		crashAndDie();
	}
	THREAD_RETURN;
}

static Future<Void> logNetIOThreadMetrics(std::vector<NetIOThread*> threads, NetIOBudget* budget) {
	struct Totals {
		int64_t reads = 0, bytesReceived = 0, writes = 0, bytesSent = 0, eventBatches = 0;
		double cpuSeconds = 0;
	};
	std::vector<Totals> last(threads.size());
	int64_t lastStops = budget->stopCount();
	double lastTime = now();
	while (FLOW_KNOBS->NET_IO_OFFLOAD_METRICS_INTERVAL > 0) {
		co_await delay(FLOW_KNOBS->NET_IO_OFFLOAD_METRICS_INTERVAL);
		const double time = now();
		for (size_t i = 0; i < threads.size(); ++i) {
			NetIOThreadStats& stats = threads[i]->getStats();
			Totals current{ stats.reads.load(),     stats.bytesReceived.load(), stats.writes.load(),
				            stats.bytesSent.load(), stats.eventBatches.load(),  threads[i]->cpuSeconds() };
			TraceEvent("NetIOThreadMetrics")
			    .detail("Thread", threads[i]->getIndex())
			    .detail("Elapsed", time - lastTime)
			    .detail("Connections", stats.connections.load())
			    .detail("Reads", current.reads - last[i].reads)
			    .detail("BytesReceived", current.bytesReceived - last[i].bytesReceived)
			    .detail("Writes", current.writes - last[i].writes)
			    .detail("BytesSent", current.bytesSent - last[i].bytesSent)
			    .detail("EventBatches", current.eventBatches - last[i].eventBatches)
			    .detail("CPUSeconds", current.cpuSeconds - last[i].cpuSeconds);
			last[i] = current;
		}
		const int64_t stops = budget->stopCount();
		TraceEvent("NetIOMetrics")
		    .detail("Elapsed", time - lastTime)
		    .detail("BufferedBytes", budget->usedBytes())
		    .detail("ProcessBufferLimitStops", stops - lastStops);
		lastStops = stops;
		lastTime = time;
	}
}

NetIOThreads::NetIOThreads(boost::asio::io_context& networkContext) : networkContext(networkContext) {}

NetIOThreads::~NetIOThreads() {
	stop();
}

NetIOThread* NetIOThreads::pick(NetIODirection direction) {
	if (FLOW_KNOBS->NET_IO_OFFLOAD_THREADS == 0 || direction == NetIODirection::None || disabled ||
	    !(direction == NetIODirection::Incoming ? FLOW_KNOBS->NET_IO_OFFLOAD_INCOMING
	                                            : FLOW_KNOBS->NET_IO_OFFLOAD_OUTGOING) ||
	    g_network->isSimulated()) {
		return nullptr;
	}
	if (FLOW_KNOBS->NET_IO_OFFLOAD_THREADS < 0 || FLOW_KNOBS->NET_IO_OFFLOAD_BUFFER_BYTES <= 0 ||
	    FLOW_KNOBS->NET_IO_OFFLOAD_PROCESS_BUFFER_BYTES <= 0 || FLOW_KNOBS->MAX_PACKET_SEND_BYTES <= 0) {
		TraceEvent(SevWarnAlways, "NetIOOffloadKnobsInvalid")
		    .suppressFor(60.0)
		    .detail("Threads", FLOW_KNOBS->NET_IO_OFFLOAD_THREADS)
		    .detail("BufferBytes", FLOW_KNOBS->NET_IO_OFFLOAD_BUFFER_BYTES)
		    .detail("ProcessBufferBytes", FLOW_KNOBS->NET_IO_OFFLOAD_PROCESS_BUFFER_BYTES)
		    .detail("MaxPacketSendBytes", FLOW_KNOBS->MAX_PACKET_SEND_BYTES);
		return nullptr;
	}
	if (threads.empty()) {
		std::vector<NetIOThread*> started;
		for (int i = 0; i < FLOW_KNOBS->NET_IO_OFFLOAD_THREADS; ++i) {
			threads.push_back(std::make_unique<NetIOThread>(i, networkContext, budget));
			started.push_back(threads.back().get());
		}
		TraceEvent("NetIOThreadsStarted").detail("Threads", threads.size());
		metricsLogger = logNetIOThreadMetrics(std::move(started), &budget);
	}
	budget.setLimit(FLOW_KNOBS->NET_IO_OFFLOAD_PROCESS_BUFFER_BYTES);
	return threads[next++ % threads.size()].get();
}

void NetIOThreads::stop() {
	disabled = true;
	metricsLogger = Future<Void>();
	for (auto& thread : threads) {
		thread->stop();
	}
}

double NetIOThreads::cpuSeconds() const {
	double total = 0;
	for (const auto& thread : threads) {
		total += thread->cpuSeconds();
	}
	return total;
}

NetIOThreads::Counters NetIOThreads::counters() const {
	Counters counters;
	counters.threadsStarted = static_cast<int64_t>(threads.size());
	for (const auto& thread : threads) {
		NetIOThreadStats& stats = thread->getStats();
		counters.offloadedIncoming += stats.offloadedIncoming.load();
		counters.offloadedOutgoing += stats.offloadedOutgoing.load();
		counters.socketsOpen += stats.connections.load();
		counters.channelsAlive += stats.channels.load();
	}
	counters.bufferedBytes = budget.usedBytes();
	counters.bufferLimitStops = budget.stopCount();
	return counters;
}

static void closeDescriptor(tcp::socket::native_handle_type fd) {
#ifdef _WIN32
	::closesocket(fd);
#else
	::close(fd);
#endif
}

std::optional<tcp::socket> moveSocketToNetIOThread(tcp::socket& socket, NetIOThread& thread) {
	error_code error;
	const tcp::endpoint local = socket.local_endpoint(error);
	if (error) {
		return {};
	}
	const tcp::socket::native_handle_type fd = socket.release(error);
	if (error) {
		return {};
	}
	tcp::socket moved(thread.context());
	if (moved.assign(local.protocol(), fd, error)) {
		closeDescriptor(fd);
	} else if (moved.non_blocking(true, error)) {
		error_code closeError;
		// NOLINTNEXTLINE(bugprone-unused-return-value)
		moved.close(closeError);
	} else {
		return moved;
	}
	TraceEvent(SevWarn, "N2_NetIOHandoffError").detail("ErrorCode", error.value()).detail("Message", error.message());
	return {};
}

NetIOConnection::NetIOConnection(NetIOThread& thread,
                                 std::unique_ptr<INetIOTransport> transport,
                                 NetIODirection direction,
                                 UID id,
                                 NetworkAddress peerAddress)
  : channel(std::make_shared<NetIOChannel>(thread, std::move(transport), *this)), id(id), peerAddress(peerAddress) {
	NetIOThreadStats& stats = thread.getStats();
	++(direction == NetIODirection::Incoming ? stats.offloadedIncoming : stats.offloadedOutgoing);
	channel->start();
}

NetIOConnection::~NetIOConnection() {
	channel->detach();
	channel->close();
}

size_t NetIOConnection::read(uint8_t* begin, uint8_t* end, error_code& error) {
	return channel->read(begin, end, error);
}

size_t NetIOConnection::write(SendBuffer const* data, int limit, error_code& error) {
	return channel->write(data, limit, error);
}

Future<Void> NetIOConnection::onReadable() {
	if (channel->readableOrWait()) {
		return Void();
	}
	if (!readable.present()) {
		readable = Promise<Void>();
	}
	return readable.get().getFuture();
}

Future<Void> NetIOConnection::onWritable() {
	if (channel->writableOrWait()) {
		return Void();
	}
	if (!writable.present()) {
		writable = Promise<Void>();
	}
	return writable.get().getFuture();
}

void NetIOConnection::close() {
	channel->close();
}

void NetIOConnection::onEvents(uint8_t events) {
	Optional<Promise<Void>> readWaiter;
	Optional<Promise<Void>> writeWaiter;
	if ((events & (NetIOChannel::Readable | NetIOChannel::Closed)) != 0 && readable.present()) {
		readWaiter = std::move(readable);
		readable.reset();
	}
	if ((events & (NetIOChannel::Writable | NetIOChannel::Closed)) != 0 && writable.present()) {
		writeWaiter = std::move(writable);
		writable.reset();
	}
	if ((events & NetIOChannel::Closed) != 0) {
		if (const error_code closeError = channel->getCloseError()) {
			TraceEvent(SevWarn, "N2_CloseError", id)
			    .suppressFor(1.0)
			    .detail("PeerAddr", peerAddress)
			    .detail("PeerAddress", peerAddress)
			    .detail("ErrorCode", closeError.value())
			    .detail("Message", closeError.message())
			    .detail("NetIOThread", true);
		}
	}
	if (readWaiter.present()) {
		readWaiter.get().send(Void());
	}
	if (writeWaiter.present()) {
		writeWaiter.get().send(Void());
	}
}

TEST_CASE("/flow/NetIO/PipeBudget") {
	NetIOBudget budget;
	budget.setLimit(int64_t{ 64 } * 1024);
	const BufferLimits limits{ size_t{ 48 } * 1024, budget };
	auto push = [](Pipe& pipe, size_t bytes) {
		Chunk* chunk = Chunk::make(bytes);
		chunk->bytes_written = static_cast<int>(bytes);
		return pipe.push(chunk);
	};
	std::vector<uint8_t> out(size_t{ 64 } * 1024);
	{
		Pipe a(limits);
		Pipe b(limits);
		while (a.room() > 0) {
			push(a, std::min(a.room(), readChunkBytes));
		}
		ASSERT_EQ(a.size(), limits.connection);
		a.stopProducer();
		ASSERT_EQ(budget.stopCount(), 0);
		ASSERT_EQ(b.room(), readChunkBytes);
		push(b, readChunkBytes);
		ASSERT_EQ(budget.usedBytes(), int64_t{ 64 } * 1024);
		ASSERT_EQ(b.room(), 0);
		b.stopProducer();
		b.stopProducer();
		ASSERT_EQ(budget.stopCount(), 1);
		ASSERT(!a.taken(a.queued.copyOut(out.data(), size_t{ 8 } * 1024)));
		ASSERT(b.taken(b.queued.copyOut(out.data(), out.size())));
		a.discard();
		ASSERT_EQ(budget.usedBytes(), 0);

		for (int i = 0; i < 10000; ++i) {
			Pipe& pipe = deterministicRandom()->coinflip() ? a : b;
			const int action = deterministicRandom()->randomInt(0, 100);
			if (action < 60) {
				const size_t room = std::min(pipe.room(), readChunkBytes);
				if (room == 0) {
					pipe.stopProducer();
				} else {
					push(pipe, deterministicRandom()->randomInt(1, static_cast<int>(room) + 1));
				}
			} else if (action < 98) {
				const size_t want = deterministicRandom()->randomInt(0, 16 * 1024 + 1);
				pipe.taken(pipe.queued.copyOut(out.data(), want));
			} else {
				pipe.discard();
			}
			ASSERT_EQ(budget.usedBytes(), static_cast<int64_t>(a.size() + b.size()));
			ASSERT_LE(budget.usedBytes(), int64_t{ 64 } * 1024 + 2 * static_cast<int64_t>(readChunkBytes));
		}
		push(a, 1);
	}
	ASSERT_EQ(budget.usedBytes(), 0);
	return Void();
}

} // namespace N2
