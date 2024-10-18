
#include "Tester.h"
#include <numeric>
#include <algorithm>
#include <iostream>
#include <thread>
#include <utility>
#include <fstream>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

template<typename T>
void serialize(const void *data, std::string &buffer, size_t);

template<>
void serialize<int>(const void *data, std::string &buffer, size_t) {
    char tempBuffer[20];
    int n = sprintf(tempBuffer, "%d", *reinterpret_cast<const int *>(data));
    buffer.append(tempBuffer, n);
}

template<>
void serialize<double>(const void *data, std::string &buffer, size_t) {
    char tempBuffer[20];
    int n = sprintf(tempBuffer, "%.2f", *reinterpret_cast<const double *>(data));
    buffer.append(tempBuffer, n);
}

template<>
void serialize<char>(const void *data, std::string &buffer, size_t) {
    buffer.push_back(*reinterpret_cast<const char *>(data));
}

template<>
void serialize<const char *>(const void *data, std::string &buffer, size_t size) {
    const char *str = reinterpret_cast<const char *>(data);
    size_t len = strnlen(str, size);
    buffer.append(str, len);
}

Tester::Tester(std::string name, xdbc::RuntimeEnv &env)
        : name(std::move(name)), env(&env), xclient(env) {

    spdlog::get("XCLIENT")->info("#1 Constructed XClient called: {0}", xclient.get_name());

    xclient.startReceiving(env.table);
    spdlog::get("XCLIENT")->info("#4 called receive");

}


void Tester::close() {

    xclient.finalize();

}


int Tester::analyticsThread(int thr, int &min, int &max, long &sum, long &cnt, long &totalcnt) {
    env->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "start"});
    int buffsRead = 0;

    size_t tupleSize = 0;
    for (const auto &attr: env->schema) {
        tupleSize += attr.size;
    }

    while (xclient.hasNext(thr)) {
        // Get next read buffer and measure the wait time
        xdbc::buffWithId curBuffWithId = xclient.getBuffer(thr);

        if (curBuffWithId.id >= 0) {
            if (curBuffWithId.iformat == 1) {
                auto dataPtr = curBuffWithId.buff;

                for (int i = 0; i < curBuffWithId.totalTuples; ++i) {
                    totalcnt++;

                    int *firstAttribute = reinterpret_cast<int *>(dataPtr);

                    /*if (*firstAttribute < 0) {
                        spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tupleNo: {1}", curBuffWithId.id, i);
                        break;
                    }*/
                    cnt++;
                    sum += *firstAttribute;
                    if (*firstAttribute < min) min = *firstAttribute;
                    if (*firstAttribute > max) max = *firstAttribute;
                    //spdlog::get("XCLIENT")->warn("Cnt {0}", cnt);

                    dataPtr += tupleSize;
                }
            } else if (curBuffWithId.iformat == 2) {

                int *v1 = reinterpret_cast<int *>(curBuffWithId.buff);
                for (int i = 0; i < curBuffWithId.totalTuples; i++) {
                    totalcnt++;
                    /*if (v1[i] < 0) {
                        spdlog::get("XCLIENT")->warn("Empty tuple at buffer: {0}, tuple_no: {1}", curBuffWithId.id, i);
                        break;
                    }*/

                    sum += v1[i];
                    if (v1[i] < min) min = v1[i];
                    if (v1[i] > max) max = v1[i];
                    cnt++;
                }

            }
            buffsRead++;

            xclient.markBufferAsRead(curBuffWithId.id);
            env->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "push"});
        } else {
            spdlog::get("XCLIENT")->warn("Write thread {0} found invalid buffer with id: {1}, buff_no: {2}",
                                         thr, curBuffWithId.id, buffsRead);
            break;
        }

    }
    env->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "end"});
    return buffsRead;

}


void Tester::runAnalytics() {

    int mins[env->write_parallelism];
    int maxs[env->write_parallelism];
    long sums[env->write_parallelism];
    long cnts[env->write_parallelism];
    long totalcnts[env->write_parallelism];

    std::thread writeThreads[env->write_parallelism];

    for (int i = 0; i < env->write_parallelism; i++) {

        mins[i] = INT32_MAX;
        maxs[i] = INT32_MIN;

        sums[i] = 0L;
        cnts[i] = 0L;
        totalcnts[i] = 0L;
        writeThreads[i] = std::thread(&Tester::analyticsThread, this, i, std::ref(mins[i]), std::ref(maxs[i]),
                                      std::ref(sums[i]), std::ref(cnts[i]), std::ref(totalcnts[i]));
    }

    for (int i = 0; i < env->write_parallelism; i++) {
        writeThreads[i].join();
    }


    int *minValuePtr = std::min_element(mins, mins + env->write_parallelism);
    int *maxValuePtr = std::max_element(maxs, maxs + env->write_parallelism);
    long sum = std::accumulate(sums, sums + env->write_parallelism, 0L);
    long cnt = std::accumulate(cnts, cnts + env->write_parallelism, 0L);
    long totalcnt = std::accumulate(totalcnts, totalcnts + env->write_parallelism, 0L);

    spdlog::get("XCLIENT")->info("totalcnt: {0}", totalcnt);
    spdlog::get("XCLIENT")->info("cnt: {0}", cnt);
    spdlog::get("XCLIENT")->info("min: {0}", *minValuePtr);
    spdlog::get("XCLIENT")->info("max: {0}", *maxValuePtr);
    spdlog::get("XCLIENT")->info("avg: {0}", (sum / (double) cnt));
}

int Tester::storageThread(int thr, const std::string &filename) {
    env->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "start"});
    //std::ofstream csvFile(filename + std::to_string(thr) + ".csv", std::ios::out);
    //std::ostringstream csvBuffer(std::ios::out | std::ios::ate);

    std::ofstream csvFile(filename + std::to_string(thr) + ".csv", std::ios::out | std::ios::binary);
    //std::ostringstream csvBuffer;
    //csvBuffer << std::setprecision(std::numeric_limits<double>::max_digits10);
    //csvBuffer << std::setprecision(10);
    //TODO: make generic
    //csvBuffer.str(std::string(env->buffer_size * 1024, '\0'));
    //csvBuffer.clear();
    std::string csvBuffer;
    csvBuffer.reserve((env->buffer_size + 100) * 1024); // Preallocate
    char firstSchemaAttChar = env->schema[0].tpe[0];


    int totalcnt = 0;
    int cnt = 0;
    int buffsRead = 0;
    char comma = ',';
    char newLine = '\n';

    size_t schemaSize = env->schema.size();

    //TODO: refactor to call only when columnar format (2)
    std::vector<size_t> offsets(schemaSize);
    size_t baseOffset = 0;
    for (size_t i = 0; i < schemaSize; ++i) {
        offsets[i] = baseOffset;
        baseOffset += env->tuples_per_buffer * env->schema[i].size;
    }

    //TODO: call only when row
    std::vector<size_t> offsetsRow(schemaSize);
    std::vector<size_t> sizes(schemaSize);
    std::vector<size_t> schemaChars(schemaSize);

    using SerializeFunc = void (*)(const void *, std::string &, size_t);
    std::vector<SerializeFunc> serializers(schemaSize);
    size_t baseOffsetRow = 0;
    for (size_t i = 0; i < schemaSize; ++i) {
        offsetsRow[i] = baseOffsetRow;
        if (env->schema[i].tpe[0] == 'I') {
            sizes[i] = 4; // sizeof(int)
            schemaChars[i] = 'I';
            serializers[i] = serialize<int>;
        } else if (env->schema[i].tpe[0] == 'D') {
            sizes[i] = 8; // sizeof(double)
            schemaChars[i] = 'D';
            serializers[i] = serialize<double>;
        } else if (env->schema[i].tpe[0] == 'C') {
            sizes[i] = 1; // sizeof(char)
            schemaChars[i] = 'C';
            serializers[i] = serialize<char>;
        } else if (env->schema[i].tpe[0] == 'S') {
            sizes[i] = env->schema[i].size;
            schemaChars[i] = 'S';
            serializers[i] = serialize<const char *>;
        }
        baseOffsetRow += sizes[i];
    }

    while (xclient.hasNext(thr)) {
        // Get next read buffer and measure the waiting time
        xdbc::buffWithId curBuffWithId = xclient.getBuffer(thr);

        if (curBuffWithId.id >= 0) {
            auto start_deser = std::chrono::high_resolution_clock::now();
            if (curBuffWithId.iformat == 1) {

                auto dataPtr = curBuffWithId.buff;
                for (size_t i = 0; i < curBuffWithId.totalTuples; ++i) {


                    //treat first attribute outside of loop to simplify delimiter handling
                    size_t curSize = sizes[0];
                    serializers[0](dataPtr, csvBuffer, curSize);
                    size_t offset = curSize;

                    for (size_t j = 1; j < schemaSize; ++j) {
                        csvBuffer.push_back(comma);
                        curSize = sizes[j];
                        const void *dataPtrOffset = dataPtr + offset;
                        serializers[j](dataPtrOffset, csvBuffer, curSize);
                        offset += curSize;

                    }
                    csvBuffer.push_back(newLine);

                    cnt++;
                    dataPtr += offset;
                }

            }
            if (curBuffWithId.iformat == 2) {
                auto dataPtr = curBuffWithId.buff;

                std::vector<void *> pointers(schemaSize);

                for (size_t j = 0; j < schemaSize; ++j) {
                    pointers[j] = dataPtr + offsets[j];
                }

                // Loop over rows
                for (int i = 0; i < curBuffWithId.totalTuples; ++i) {

                    //treat first attribute outside of loop to simplify delimiter handling
                    size_t curSize = sizes[0];
                    serializers[0](reinterpret_cast<const char *>(pointers[0]) + i * curSize, csvBuffer, curSize);
                    for (size_t j = 1; j < schemaSize; ++j) {
                        csvBuffer.push_back(comma);
                        curSize = sizes[j];
                        serializers[j](reinterpret_cast<const char *>(pointers[j]) + i * curSize, csvBuffer, curSize);
                    }
                    csvBuffer.push_back(newLine);
                }
            }

            //spdlog::get("XCLIENT")->info("csv buffer size {0} ", csvBuffer.size());
            csvFile.write(csvBuffer.data(), csvBuffer.size());
            csvBuffer.clear();

            buffsRead++;

            xclient.markBufferAsRead(curBuffWithId.id);
            env->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "push"});
        } else {
            spdlog::get("XCLIENT")->warn("found invalid buffer with id: {0}, buff_no: {1}",
                                         curBuffWithId.id, buffsRead);
            break;
        }

    }

    //spdlog::get("XCLIENT")->info("Thr {0} DeserTime: {1}, WriteTime {2}", thr, deserTime / 1000, writeTime / 1000);

    spdlog::get("XCLIENT")->info("Write thread {0} Total written buffers: {1}", thr, buffsRead);
    csvFile.close();

    env->pts->push(xdbc::ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "write", "end"});
    return buffsRead;
}


void Tester::runStorage(const std::string &filename) {

    std::thread writeThreads[env->write_parallelism];

    for (int i = 0; i < env->write_parallelism; i++) {
        writeThreads[i] = std::thread(&Tester::storageThread, this, i, std::ref(filename));
    }


    for (int i = 0; i < env->write_parallelism; i++) {
        writeThreads[i].join();
    }

    //TODO: combine multiple files or maybe do in external bash script
    /*std::ofstream csvFile(filename, std::ios::out);
    csvFile.seekp(0, std::ios::end);
    std::streampos fileSize = csvFile.tellp();
    spdlog::get("XCLIENT")->info("fileSize: {0}", fileSize);
    csvFile.close();*/


}