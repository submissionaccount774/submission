#ifndef XDBC_XCLIENT_H
#define XDBC_XCLIENT_H

#include <string>
#include <vector>
#include <array>
#include <atomic>
#include <thread>
#include <stack>
#include <boost/asio.hpp>
#include <set>
#include "customQueue.h"
#include "utils.h"

using namespace boost::asio;
using ip::tcp;

namespace xdbc {

    constexpr size_t MAX_ATTRIBUTES = 230;
    struct Header {

        size_t compressionType;
        size_t totalSize;
        size_t totalTuples;
        size_t intermediateFormat;
        size_t crc;
        size_t attributeSize[MAX_ATTRIBUTES];
        size_t attributeComp[MAX_ATTRIBUTES];

    };

    struct SchemaAttribute {
        std::string name;
        std::string tpe;
        int size;
    };

    struct ProfilingTimestamps {
        std::chrono::high_resolution_clock::time_point timestamp;
        int thread;
        std::string component;
        std::string event;
    };

    typedef std::shared_ptr<customQueue<int>> FBQ_ptr;
    typedef std::shared_ptr<customQueue<ProfilingTimestamps>> PTQ_ptr;

    struct RuntimeEnv {
        long transfer_id;
        std::string env_name;
        int buffers_in_bufferpool;
        int buffer_size;
        int tuples_per_buffer;
        int tuple_size;
        int iformat;
        std::chrono::milliseconds sleep_time;
        int rcv_parallelism;
        int decomp_parallelism;
        int write_parallelism;
        std::chrono::steady_clock::time_point startTime;
        std::string table;
        std::string server_host;
        std::string server_port;
        std::vector<SchemaAttribute> schema;
        std::string schemaJSON;
        std::vector<FBQ_ptr> freeBufferIds;
        std::vector<FBQ_ptr> compressedBufferIds;
        std::vector<FBQ_ptr> decompressedBufferIds;
        int mode;
        std::vector<std::tuple<long long, size_t, size_t, size_t>> queueSizes;
        std::atomic<bool> monitor;

        PTQ_ptr pts;
    };

    struct buffWithId {
        int id;
        int iformat;
        size_t totalTuples;
        std::byte *buff;
    };

    class XClient {
    private:

        RuntimeEnv *_xdbcenv;
        std::atomic<int> _readState;
        std::vector<std::vector<std::byte>> _bufferPool;
        std::vector<std::atomic<bool>> _consumedAll;
        std::atomic<int> _totalBuffersRead;
        std::vector<std::thread> _rcvThreads;
        std::vector<std::thread> _decompThreads;
        std::vector<ip::tcp::socket> _readSockets;
        boost::asio::io_context _ioContext;
        boost::asio::ip::tcp::socket _baseSocket;
        std::vector<int> _emptyDecompThreadCtr;
        std::atomic<int> _markedFreeCounter;
        std::thread _monitorThread;

    public:

        XClient(RuntimeEnv &xdbcenv);

        std::string get_name() const;

        void receive(int threadno);

        void decompress(int threadno);

        void initialize(const std::string &tableName);

        int startReceiving(const std::string &tableName);

        bool hasNext(int readThread);

        buffWithId getBuffer(int readThread);

        int getBufferPoolSize() const;

        void finalize();

        void markBufferAsRead(int buffId);

        void monitorQueues(int interval_ms);

    };

}

#endif //XDBC_XCLIENT_H
