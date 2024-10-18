#include "xclient.h"
#include <boost/asio.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <thread>
#include <chrono>
#include <algorithm>
#include <iterator>
#include <fstream>
#include <iomanip>
#include "spdlog/spdlog.h"
#include "spdlog/stopwatch.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "Decompression/Decompressor.h"
#include "xdbc/metrics_calculator.h"

using namespace boost::asio;
using ip::tcp;

namespace xdbc {


    XClient::XClient(RuntimeEnv &env) :
            _xdbcenv(&env),
            _bufferPool(),
            _readState(0),
            _totalBuffersRead(0),
            _decompThreads(env.decomp_parallelism),
            _rcvThreads(env.rcv_parallelism),
            _readSockets(),
            _emptyDecompThreadCtr(env.write_parallelism),
            _markedFreeCounter(0),
            _baseSocket(_ioContext) {

        auto console_logger = spdlog::get("XDBC.CLIENT");

        if (!console_logger) {
            // Logger does not exist, create it
            console_logger = spdlog::stdout_color_mt("XDBC.CLIENT");
        }

        PTQ_ptr pq(new customQueue<ProfilingTimestamps>);
        env.pts = pq;


        spdlog::get("XDBC.CLIENT")->info("Creating Client: {0}, BPS: {1}, BS: {2}, TS: {3}, iformat: {4} ",
                                         _xdbcenv->env_name, env.buffers_in_bufferpool, env.buffer_size, env.tuple_size,
                                         env.iformat);

        // populate bufferpool with empty vectors (header + payload)
        _bufferPool.resize(env.buffers_in_bufferpool,
                           std::vector<std::byte>(sizeof(Header) + env.tuples_per_buffer * env.tuple_size));


        for (int i = 0; i < env.write_parallelism; i++) {
            _emptyDecompThreadCtr[i] = 0;
        }
    }

    void XClient::finalize() {

        spdlog::get("XDBC.CLIENT")->info(
                "Finalizing XClient: {0}, shutting down {1} receive threads & {2} decomp threads",
                _xdbcenv->env_name, _xdbcenv->rcv_parallelism, _xdbcenv->decomp_parallelism);

        _xdbcenv->monitor.store(false);
        _monitorThread.join();

        for (int i = 0; i < _xdbcenv->decomp_parallelism; i++) {
            _decompThreads[i].join();
        }

        for (int i = 0; i < _xdbcenv->rcv_parallelism; i++) {
            _rcvThreads[i].join();
        }

        _baseSocket.close();
        spdlog::get("XDBC.CLIENT")->info("Finalizing: basesocket closed");

        auto end = std::chrono::steady_clock::now();
        auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - _xdbcenv->startTime).count();
        spdlog::get("XDBC.CLIENT")->info("Total elapsed time: {0} ms", total_time);


        auto pts = std::vector<xdbc::ProfilingTimestamps>(_xdbcenv->pts->size());
        while (_xdbcenv->pts->size() != 0)
            pts.push_back(_xdbcenv->pts->pop());

        auto component_metrics = calculate_metrics(pts, _xdbcenv->buffer_size);
        std::ostringstream totalTimes;
        std::ostringstream procTimes;
        std::ostringstream waitingTimes;
        std::ostringstream totalThroughput;
        std::ostringstream perBufferThroughput;

        for (const auto &[component, metrics]: component_metrics) {

            if (!component.empty()) {
                totalTimes << component << ":\t" << metrics.overall_time_ms << "ms, ";
                procTimes << component << ":\t" << metrics.processing_time_ms << "ms, ";
                waitingTimes << component << ":\t" << metrics.waiting_time_ms << "ms, ";
                totalThroughput << component << ":\t" << metrics.total_throughput << "mb/s, ";
                perBufferThroughput << component << ":\t" << metrics.per_buffer_throughput << "mb/s, ";
            }

        }

        spdlog::get("XDBC.CLIENT")->info(
                "xdbc client | \n all:\t {} \n proc:\t{} \n wait:\t{} \n thr:\t {} \n thr/b:\t {}",
                totalTimes.str(), procTimes.str(), waitingTimes.str(), totalThroughput.str(),
                perBufferThroughput.str());

        auto loads = printAndReturnAverageLoad(*_xdbcenv);

        const std::string filename = "/tmp/xdbc_client_timings.csv";

        std::ostringstream headerStream;
        headerStream << "transfer_id,total_time,"
                     << "rcv_wait_time,rcv_proc_time,rcv_throughput,rcv_throughput_pb,rcv_load,"
                     << "decomp_wait_time,decomp_proc_time,decomp_throughput,decomp_throughput_pb,decomp_load,"
                     << "write_wait_time,write_proc_time,write_throughput,write_throughput_pb,write_load\n";

        std::ifstream file_check(filename);
        bool is_empty = file_check.peek() == std::ifstream::traits_type::eof();
        file_check.close();

        std::ofstream csv_file(filename,
                               std::ios::out | std::ios::app);

        if (is_empty)
            csv_file << headerStream.str();

        csv_file << std::fixed << std::setprecision(2)
                 << std::to_string(_xdbcenv->transfer_id) << "," << total_time << ","
                 << component_metrics["rcv"].waiting_time_ms << ","
                 << component_metrics["rcv"].processing_time_ms << ","
                 << component_metrics["rcv"].total_throughput << ","
                 << component_metrics["rcv"].per_buffer_throughput << ","
                 << std::get<0>(loads) << ","
                 << component_metrics["decomp"].waiting_time_ms << ","
                 << component_metrics["decomp"].processing_time_ms << ","
                 << component_metrics["decomp"].total_throughput << ","
                 << component_metrics["decomp"].per_buffer_throughput << ","
                 << std::get<1>(loads) << ","
                 << component_metrics["write"].waiting_time_ms << ","
                 << component_metrics["write"].processing_time_ms << ","
                 << component_metrics["write"].total_throughput << ","
                 << component_metrics["write"].per_buffer_throughput << ","
                 << std::get<2>(loads) << "\n";
        csv_file.close();

    }


    std::string XClient::get_name() const {
        return _xdbcenv->env_name;
    }

    std::string read_(tcp::socket &socket) {
        boost::asio::streambuf buf;
        boost::system::error_code error;
        size_t bytes = boost::asio::read_until(socket, buf,"\n", error);

        if (error) {
            spdlog::get("XDBC.CLIENT")->warn("Boost error while reading: {0} ", error.message());
        }
        std::string data = boost::asio::buffer_cast<const char *>(buf.data());
        return data;
    }

    int XClient::startReceiving(const std::string &tableName) {

        //establish base connection with server
        XClient::initialize(tableName);

        _xdbcenv->monitor.store(true);

        _monitorThread = std::thread(&XClient::monitorQueues, this, 1000);

        //create rcv threads
        for (int i = 0; i < _xdbcenv->rcv_parallelism; i++) {
            FBQ_ptr q(new customQueue<int>);
            _xdbcenv->freeBufferIds.push_back(q);
            //initially all buffers are free to write into
            for (int j = i * (_xdbcenv->buffers_in_bufferpool / _xdbcenv->rcv_parallelism);
                 j < (i + 1) * (_xdbcenv->buffers_in_bufferpool / _xdbcenv->rcv_parallelism);
                 j++)
                q->push(j);

            _rcvThreads[i] = std::thread(&XClient::receive, this, i);
        }

        for (int i = 0; i < _xdbcenv->decomp_parallelism; i++) {
            FBQ_ptr q(new customQueue<int>);
            _xdbcenv->compressedBufferIds.push_back(q);
            _decompThreads[i] = std::thread(&XClient::decompress, this, i);
        }

        for (int i = 0; i < _xdbcenv->write_parallelism; i++) {
            FBQ_ptr q(new customQueue<int>);
            _xdbcenv->decompressedBufferIds.push_back(q);
        }

        spdlog::get("XDBC.CLIENT")->info("#3 Initialized");


        return 1;
    }

    void XClient::monitorQueues(int interval_ms) {

        long long curTimeInterval = interval_ms / 1000;

        while (_xdbcenv->monitor) {
            auto now = std::chrono::high_resolution_clock::now();
            auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

            // Calculate the total size of all queues in each category
            size_t freeBufferTotalSize = 0;
            for (auto &queue_ptr: _xdbcenv->freeBufferIds) {
                freeBufferTotalSize += queue_ptr->size();
            }

            size_t compressedBufferTotalSize = 0;
            for (auto &queue_ptr: _xdbcenv->compressedBufferIds) {
                compressedBufferTotalSize += queue_ptr->size();
            }

            size_t decompressedBufferTotalSize = 0;
            for (auto &queue_ptr: _xdbcenv->decompressedBufferIds) {
                decompressedBufferTotalSize += queue_ptr->size();
            }

            // Store the measurement as a tuple
            _xdbcenv->queueSizes.emplace_back(curTimeInterval, freeBufferTotalSize, compressedBufferTotalSize,
                                              decompressedBufferTotalSize);

            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
            curTimeInterval += interval_ms / 1000;
        }
    }


    void XClient::initialize(const std::string &tableName) {

        //this is for IP address
        /*boost::asio::io_service io_service;
        //socket creation
        ip::tcp::socket socket(io_service);
        socket.connect(tcp::endpoint(boost::asio::ip::address::from_string("127.0.0.1"), 1234));
         */

        //this is for hostname

        boost::asio::ip::tcp::resolver resolver(_ioContext);
        boost::asio::ip::tcp::resolver::query query(_xdbcenv->server_host, _xdbcenv->server_port);
        boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query);
        boost::asio::ip::tcp::endpoint endpoint = iter->endpoint();

        spdlog::get("XDBC.CLIENT")->info("Basesocket: trying to connect");

        boost::system::error_code ec;
        _baseSocket.connect(endpoint, ec);

        int tries = 0;
        while (ec && tries < 3) {
            spdlog::get("XDBC.CLIENT")->warn("Basesocket not connecting, trying to reconnect...");
            tries++;
            _baseSocket.close();
            std::this_thread::sleep_for(_xdbcenv->sleep_time * 10);
            _baseSocket.connect(endpoint, ec);

        }

        if (ec) {
            spdlog::get("XDBC.CLIENT")->error("Failed to connect after retries: {0}", ec.message());
            throw boost::system::system_error(ec);  // Explicitly throw if connection fails
        }

        spdlog::get("XDBC.CLIENT")->info("Basesocket: connected to {0}:{1}",
                                         endpoint.address().to_string(), endpoint.port());

        boost::system::error_code error;
        const std::string &msg = tableName;
        std::uint32_t tableNameSize = msg.size();
        std::vector<boost::asio::const_buffer> tableNameBuffers;

        tableNameBuffers.emplace_back(boost::asio::buffer(&tableNameSize, sizeof(tableNameSize)));
        tableNameBuffers.emplace_back(boost::asio::buffer(msg));

        boost::asio::write(_baseSocket, tableNameBuffers, error);


        std::uint32_t data_size = _xdbcenv->schemaJSON.size();
        std::vector<boost::asio::const_buffer> buffers;
        buffers.emplace_back(boost::asio::buffer(&data_size, sizeof(data_size)));
        buffers.emplace_back(boost::asio::buffer(_xdbcenv->schemaJSON));

        boost::asio::write(_baseSocket, buffers, error);

        //std::this_thread::sleep_for(_xdbcenv->sleep_time*10);
        std::string ready = read_(_baseSocket);

        //TODO: make a check that server is actually ready and try again until ready
        //ready.erase(std::remove(ready.begin(), ready.end(), '\n'), ready.cend());
        spdlog::get("XDBC.CLIENT")->info("Basesocket: Server signaled: {0}", ready);

        //return socket;

    }


    void XClient::receive(int thr) {
        _xdbcenv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "rcv", "start"});
        spdlog::get("XDBC.CLIENT")->info("Entered receive thread {0} ", thr);
        boost::asio::io_service io_service;
        ip::tcp::socket socket(io_service);
        boost::asio::ip::tcp::resolver resolver(io_service);
        boost::asio::ip::tcp::resolver::query query(_baseSocket.remote_endpoint().address().to_string(),
                                                    std::to_string(stoi(_xdbcenv->server_port) + thr + 1));
        boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query);
        boost::asio::ip::tcp::endpoint endpoint = iter->endpoint();

        bool connected = false;

        try {
            socket.connect(endpoint);
            connected = true;
            spdlog::get("XDBC.CLIENT")->info("Receive thread {0} connected to {1}:{2}",
                                             thr, endpoint.address().to_string(), endpoint.port());

        } catch (const boost::system::system_error &error) {
            spdlog::get("XDBC.CLIENT")->warn("Server error: {0}", error.what());
            //std::this_thread::sleep_for(_xdbcenv->sleep_time);
        }

        if (connected) {

            const std::string msg = std::to_string(thr) + "\n";
            boost::system::error_code error;

            try {
                size_t b = boost::asio::write(socket, boost::asio::buffer(msg), error);
            } catch (const boost::system::system_error &e) {
                spdlog::get("XDBC.CLIENT")->warn("Could not write thread no, error: {0}", e.what());
            }

            //partition read threads
            //int minBId = thr * (_xdbcenv->bufferpool_size / _xdbcenv->rcv_parallelism);
            //int maxBId = (thr + 1) * (_xdbcenv->bufferpool_size / _xdbcenv->rcv_parallelism);

            //spdlog::get("XDBC.CLIENT")->info("Read thread {0} assigned ({1},{2})", thr, minBId, maxBId);

            int bpi;
            int buffers = 0;

            spdlog::get("XDBC.CLIENT")->info("Receive thread {0} started", thr);

            size_t headerBytes;
            size_t readBytes;

            int decompThreadId = 0;

            while (error != boost::asio::error::eof) {

                //_readState.store(0);

                bpi = _xdbcenv->freeBufferIds[thr]->pop();
                _xdbcenv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "rcv", "pop"});

                //_readState.store(1);

                // getting response from server. Start with reading header and measuring header receive time.

                //Header header{};
                //std::memcpy(_bufferPool[bpi].data(), &header, sizeof(Header));
                headerBytes = boost::asio::read(socket, boost::asio::buffer(_bufferPool[bpi].data(), sizeof(Header)),
                                                boost::asio::transfer_exactly(sizeof(Header)), error);
                Header header = *reinterpret_cast<Header *>(_bufferPool[bpi].data());

                //uint16_t checksum = header.crc;

                //TODO: handle error types (e.g., EOF)
                if (error) {
                    spdlog::get("XDBC.CLIENT")->error("Receive thread {0}: boost error while reading header: {1}", thr,
                                                      error.message());
                    spdlog::get("XDBC.CLIENT")->error("header comp: {0}, size: {1}, tuples {2}, headerBytes: {3}",
                                                      header.compressionType,
                                                      header.totalSize,
                                                      header.totalTuples,
                                                      headerBytes);

                    if (error == boost::asio::error::eof) {
                        spdlog::get("XDBC.CLIENT")->error("EOF");
                    }
                    break;
                }

                //_readState.store(2);

                // check for errors in header
                if (header.compressionType > 6)
                    spdlog::get("XDBC.CLIENT")->error("Client: corrupt header: comp: {0}, size: {1}, headerbytes: {2}",
                                                      header.compressionType, header.totalSize, headerBytes);
                if (header.totalSize > _xdbcenv->tuples_per_buffer * _xdbcenv->tuple_size)
                    spdlog::get("XDBC.CLIENT")->error(
                            "Client: corrupt body: comp: {0}, size: {1}/{2}, headerbytes: {3}",
                            header.compressionType, header.totalSize,
                            _xdbcenv->tuples_per_buffer * _xdbcenv->tuple_size, headerBytes);

                // all good, read incoming body and measure time
                //std::vector<std::byte> compressed_buffer(sizeof(Header) + header.totalSize);


                readBytes = boost::asio::read(socket, boost::asio::buffer(_bufferPool[bpi].data() + sizeof(Header),
                                                                          header.totalSize),
                                              boost::asio::transfer_exactly(header.totalSize), error);


                // check for errors in body
                //TODO: handle errors correctly

                if (error) {
                    spdlog::get("XDBC.CLIENT")->error(
                            "Client: boost error while reading body: readBytes {0}, error: {1}",
                            readBytes, error.message());
                    if (error == boost::asio::error::eof) {

                    }
                    break;
                }


                //printSl(reinterpret_cast<shortLineitem *>(compressed_buffer.data()));

                _totalBuffersRead.fetch_add(1);
                _xdbcenv->compressedBufferIds[decompThreadId]->push(bpi);
                _xdbcenv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "rcv", "push"});

                decompThreadId = (decompThreadId + 1) % _xdbcenv->decomp_parallelism;

                buffers++;

                //_readState.store(3);
            }

            /*for (auto x: _bufferPool[bpi])
                printSl(&x);*/

            //_readState.store(4);

            for (int i = 0; i < _xdbcenv->decomp_parallelism; i++)
                _xdbcenv->compressedBufferIds[i]->push(-1);

            socket.close();

            spdlog::get("XDBC.CLIENT")->info("Receive thread {0} #buffers: {1}", thr, buffers);
        } else
            spdlog::get("XDBC.CLIENT")->error("Receive thread {0} could not connect", thr);

        _xdbcenv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "rcv", "end"});
    }

    void XClient::decompress(int thr) {
        _xdbcenv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "decomp", "start"});

        int readThrId = 0;
        int emptyCtr = 0;
        int decompError;
        int buffersDecompressed = 0;
        std::vector<char> decompressed_buffer(_xdbcenv->tuples_per_buffer * _xdbcenv->tuple_size);

        while (emptyCtr < _xdbcenv->rcv_parallelism) {

            int compBuffId = _xdbcenv->compressedBufferIds[thr]->pop();
            _xdbcenv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "decomp", "pop"});

            // decompress the specific buffer depending on the type (header or not, type of header,...) and measure time

            if (compBuffId == -1)
                emptyCtr++;
            else {
                size_t totalTuples;

                Header *header = reinterpret_cast<Header *>(_bufferPool[compBuffId].data());
                std::byte *compressed_buffer = _bufferPool[compBuffId].data() + sizeof(Header);

                if (header->compressionType > 0) {

                    //TODO: refactor decompress_cols with schema in Decompressor
                    if (header->compressionType == 6) {

                        int posInWriteBuffer = 0;
                        size_t posInReadBuffer = 0;
                        int i = 0;

                        for (const auto &attribute: _xdbcenv->schema) {

                            //spdlog::get("XDBC.CLIENT")->warn("Handling att: {0}", std::get<0>(attribute));

                            if (attribute.tpe == "INT") {

                                decompError = Decompressor::decompress_int_col(
                                        reinterpret_cast<const uint32_t *>(compressed_buffer) +
                                        posInReadBuffer / attribute.size,
                                        header->attributeSize[i] / attribute.size,
                                        decompressed_buffer.data() + posInWriteBuffer,
                                        _xdbcenv->tuples_per_buffer);

                            } else if (attribute.tpe == "DOUBLE") {
                                /*spdlog::get("XDBC.CLIENT")->warn("Handling att: {0}, compressed_size: {1}",
                                                                 std::get<0>(attribute), attSize);*/

                                /*decompError = Decompressor::decompress_zstd(
                                        decompressed_buffer.data() + posInWriteBuffer,
                                        compressed_buffer + posInReadBuffer,
                                        header->attributeSize[i],
                                        _xdbcenv->buffer_size * 8);*/

                                decompError = Decompressor::decompress_double_col(
                                        compressed_buffer + posInReadBuffer,
                                        header->attributeSize[i],
                                        decompressed_buffer.data() + posInWriteBuffer,
                                        _xdbcenv->tuples_per_buffer);

                            }
                            //TODO: add CHAR, STRING decompression
                            posInWriteBuffer += _xdbcenv->tuples_per_buffer * attribute.size;
                            posInReadBuffer += header->attributeSize[i];
                            i++;
                        }

                    } else
                        decompError = Decompressor::decompress(header->compressionType, decompressed_buffer.data(),
                                                               compressed_buffer, header->totalSize,
                                                               _xdbcenv->tuples_per_buffer * _xdbcenv->tuple_size);

                    if (decompError == 1) {

                        //TODO: check if we can just skip the buffer
                        /*size_t computed_checksum = compute_crc(boost::asio::buffer(_bufferPool[bpi]));
                        if (computed_checksum != checksum) {
                            spdlog::get("XDBC.CLIENT")->warn("CHECKSUM MISMATCH expected: {0}, got: {1}",
                                                             checksum, computed_checksum);
                        }*/

                        size_t offset = 0;
                        int m2i = -2;
                        double m2d = -2.0;
                        char m2c = ' ';

                        if (header->intermediateFormat == 1) {
                            for (const auto &attr: _xdbcenv->schema) {
                                if (attr.tpe == "INT") {
                                    std::memcpy(decompressed_buffer.data() + offset, &m2i, attr.size);
                                    offset += attr.size;
                                } else if (attr.tpe == "DOUBLE") {
                                    std::memcpy(decompressed_buffer.data() + offset, &m2d, attr.size);
                                    offset += attr.size;
                                } else if (attr.tpe == "CHAR") {
                                    std::memcpy(decompressed_buffer.data() + offset, &m2c, attr.size);
                                    offset += attr.size;
                                } else if (attr.tpe == "STRING") {
                                    std::string m2s(attr.size, ' ');
                                    m2s.back() = '\0';
                                    std::memcpy(decompressed_buffer.data() + offset, m2s.data(), attr.size);
                                    offset += attr.size;
                                }
                            }
                        } else if (header->intermediateFormat == 2) {
                            for (const auto &attr: _xdbcenv->schema) {
                                if (attr.tpe == "INT") {
                                    std::memcpy(decompressed_buffer.data() + offset, &m2i, attr.size);
                                    offset += _xdbcenv->tuples_per_buffer * attr.size;
                                } else if (attr.tpe == "DOUBLE") {
                                    std::memcpy(decompressed_buffer.data() + offset, &m2d, attr.size);
                                    offset += _xdbcenv->tuples_per_buffer * attr.size;
                                } else if (attr.tpe == "CHAR") {
                                    std::memcpy(decompressed_buffer.data() + offset, &m2c, attr.size);
                                    offset += _xdbcenv->tuples_per_buffer * attr.size;
                                } else if (attr.tpe == "STRING") {
                                    std::string m2s(attr.size, ' ');
                                    m2s.back() = '\0';
                                    std::memcpy(decompressed_buffer.data() + offset, m2s.data(), attr.size);
                                    offset += attr.size;
                                }
                            }

                        }

                        spdlog::get("XDBC.CLIENT")->warn(
                                "decompress error: header: comp: {0}, size: {1}",
                                header->compressionType, header->totalSize);

                    }

                    //write totaltuples as temp header
                    memcpy(_bufferPool[compBuffId].data(), &header->totalTuples, sizeof(size_t));
                    memcpy(_bufferPool[compBuffId].data() + sizeof(size_t), decompressed_buffer.data(),
                           _xdbcenv->tuple_size * _xdbcenv->tuples_per_buffer);

                } else if (header->compressionType == 0) {
                    //write totaltuples as temp header
                    memcpy(_bufferPool[compBuffId].data(), &header->totalTuples, sizeof(size_t));
                    memmove(_bufferPool[compBuffId].data() + sizeof(size_t), compressed_buffer, header->totalSize);
                }


                _xdbcenv->decompressedBufferIds[readThrId]->push(compBuffId);
                _xdbcenv->pts->push(
                        ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "decomp", "push"});
                buffersDecompressed++;

                readThrId++;
                if (readThrId == _xdbcenv->write_parallelism)
                    readThrId = 0;
            }
        }

        for (int i = 0; i < _xdbcenv->write_parallelism; i++)
            _xdbcenv->decompressedBufferIds[i]->push(-1);

        spdlog::get("XDBC.CLIENT")->warn("Decomp thread {0} finished", thr);
        _xdbcenv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "decomp", "end"});
    }

    //TODO: handle parallelism internally
    bool XClient::hasNext(int readThreadId) {
        if (_emptyDecompThreadCtr[readThreadId] == _xdbcenv->decomp_parallelism)
            return false;

        return true;
    }

    //TODO: handle parallelism internally
    buffWithId XClient::getBuffer(int readThreadId) {

        int buffId = _xdbcenv->decompressedBufferIds[readThreadId]->pop();
        _xdbcenv->pts->push(
                ProfilingTimestamps{std::chrono::high_resolution_clock::now(), readThreadId, "write", "pop"});

        buffWithId curBuf{};
        if (buffId == -1)
            _emptyDecompThreadCtr[readThreadId]++;

        size_t totalTuples = 0;
        if (buffId > -1) {



            std::memcpy(&totalTuples, _bufferPool[buffId].data(), sizeof(size_t));


            curBuf.buff = _bufferPool[buffId].data() + sizeof(size_t);

        }
        curBuf.id = buffId;
        curBuf.totalTuples = totalTuples;
        //TODO: set intermediate format dynamically
        curBuf.iformat = _xdbcenv->iformat;

        //spdlog::get("XDBC.CLIENT")->warn("Sending buffer {0} to read thread {1}", buffId, readThreadId);


        return curBuf;
    }

    int XClient::getBufferPoolSize() const {
        return _xdbcenv->buffers_in_bufferpool;
    }

    void XClient::markBufferAsRead(int buffId) {
        //TODO: ensure equal distribution
        //spdlog::get("XDBC.CLIENT")->warn("freeing {0} for {1}", buffId, _markedFreeCounter % _xdbcenv->rcv_parallelism);
        _xdbcenv->freeBufferIds[_markedFreeCounter % _xdbcenv->rcv_parallelism]->push(buffId);
        _markedFreeCounter.fetch_add(1);
    }

}
