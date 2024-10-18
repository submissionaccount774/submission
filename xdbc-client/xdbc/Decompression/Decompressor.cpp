#include "Decompressor.h"

#include <boost/asio.hpp>
#include "spdlog/spdlog.h"
#include <zstd.h>
#include <snappy.h>
#include <lzo/lzo1x.h>
#include <lz4.h>
#include <zlib.h>
#include <fastpfor/codecfactory.h>
#include <fastpfor/deltautil.h>
#include <fpzip.h>

int Decompressor::decompress(int method, void *dst, const void *src, size_t in_size, int out_size) {
    //1 zstd
    //2 snappy
    //3 lzo
    //4 lz4
    //5 zlib

    if (method == 1)
        return Decompressor::decompress_zstd(dst, src, in_size, out_size);

    if (method == 2)
        return Decompressor::decompress_snappy(dst, src, in_size, out_size);

    if (method == 3)
        return Decompressor::decompress_lzo(dst, src, in_size, out_size);

    if (method == 4)
        return Decompressor::decompress_lz4(dst, src, in_size, out_size);

    if (method == 5)
        return Decompressor::decompress_zlib(dst, src, in_size, out_size);


    return 1;
}

int Decompressor::decompress_zstd(void *dst, const void *src, size_t in_size, int out_size) {
    int ret = 0;
    //TODO: fix first 2 fields for zstd
    //TODO: move decompression context outside of this function and pass it
    ZSTD_DCtx *dctx = ZSTD_createDCtx(); // create a decompression context

    // Get the raw buffer pointer and size
    //const char* compressed_data = boost::asio::buffer_cast<const char*>(in);
    //size_t in_size = boost::asio::buffer_size(in);

    size_t decompressed_max_size = ZSTD_getFrameContentSize(src, in_size);
    size_t decompressed_size = ZSTD_decompressDCtx(dctx, dst, out_size, src, in_size);
    //cout << "decompressed: " << decompressed_size << endl;
    /*uint* int_ptr = static_cast<uint*>(dst);
    int val = *int_ptr;
    cout << "l_orderkey" << val << endl;*/
    // Resize the buffer to the decompressed size
    //buffer = boost::asio::buffer(data, result);

    if (ZSTD_isError(decompressed_size)) {
        spdlog::get("XDBC.CLIENT")->warn("ZSTD decompression error: {0}", ZSTD_getErrorName(decompressed_size));
        ret = 1;
    }

    return ret;
}

int Decompressor::decompress_snappy(void *dst, const void *src, size_t in_size, int out_size) {
    int ret = 0;

    const char *data = reinterpret_cast<const char *>(src);


    // Determine the size of the uncompressed data
    size_t uncompressed_size;
    if (!snappy::GetUncompressedLength(data, in_size, &uncompressed_size)) {
        spdlog::get("XDBC.CLIENT")->error("Snappy: failed to get uncompressed size");
        //throw std::runtime_error("failed to get uncompressed size");
        ret = 1;
    }

    // Decompress the data into the provided destination
    if (!snappy::RawUncompress(data, in_size, static_cast<char *>(dst))) {
        spdlog::get("XDBC.CLIENT")->error("Snappy: failed to decompress data");
        //throw std::runtime_error("Client: Snappy: failed to decompress data");
        ret = 1;
    }

    return ret;
}

int Decompressor::decompress_lzo(void *dst, const void *src, size_t in_size, int out_size) {
    int ret = 0;

    auto *data = reinterpret_cast<const unsigned char *>(src);

    //std::size_t in_size = boost::asio::buffer_size(in);

    // Estimate the worst-case size of the decompressed data
    std::size_t max_uncompressed_size = in_size;

    // Decompress the data
    int result = lzo1x_decompress(data,
                                  in_size,
                                  reinterpret_cast<unsigned char *>(dst),
                                  &max_uncompressed_size,
                                  nullptr
    );

    if (result != LZO_E_OK) {
        spdlog::get("XDBC.CLIENT")->error("lzo: failed to decompress data, error {0}", result);
        // Handle error
        ret = 1;
    }

    if (max_uncompressed_size != out_size) {
        spdlog::get("XDBC.CLIENT")->error("lzo: failed, max_size: {0}, out_size {1}",
                                          max_uncompressed_size, out_size);
        // Handle error
        ret = 1;
    }
    return ret;
}

int Decompressor::decompress_lz4(void *dst, const void *src, size_t in_size, int out_size) {
    int ret = 0;
    auto *data = reinterpret_cast<const char *>(src);

    // Get the size of the uncompressed data
    int uncompressed_size = LZ4_decompress_safe(data, static_cast<char *>(dst),
                                                in_size, out_size);

    if (uncompressed_size < 0) {
        spdlog::get("XDBC.CLIENT")->error("LZ4: Failed to decompress data, uncompressed_size<0");
        ret = 1;
    } else if (uncompressed_size !=
               LZ4_decompress_safe(data, static_cast<char *>(dst), static_cast<int>(in_size),
                                   uncompressed_size)) {
        spdlog::get("XDBC.CLIENT")->error(
                "Failed to decompress LZ4 data: uncompressed size doesn't match expected size");
    }
    return ret;
}

int Decompressor::decompress_zlib(void *dst, const void *src, size_t in_size, int out_size) {
    int ret = 0;
    // Get the underlying data pointer and size from the const_buffer
    const char *compressed_data = static_cast<const char *>(src);

    // Initialize zlib stream
    z_stream stream{};
    stream.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(compressed_data));
    stream.avail_in = static_cast<uInt>(in_size);
    stream.avail_out = out_size;
    stream.next_out = reinterpret_cast<Bytef *>(dst);


    // Initialize zlib for decompression
    if (inflateInit(&stream) != Z_OK) {
        spdlog::get("XDBC.CLIENT")->error("ZLIB: Failed to initialize zlib for decompression ");
        ret = 1;
    }

    // Decompress the data
    int retZ = inflate(&stream, Z_FINISH);
    if (retZ != Z_STREAM_END) {
        spdlog::get("XDBC.CLIENT")->error("ZLIB: Decompression failed with error code: {0}", zError(retZ));
        ret = 1;
    }

    // Clean up zlib resources
    inflateEnd(&stream);
    return ret;
}

int Decompressor::decompress_int_col(const void *src, size_t compressed_size, void *dst, int bufferSize) {


    using namespace FastPForLib;
    CODECFactory factory;
    IntegerCODEC &codec = *factory.getFromName("simdfastpfor256");
    std::vector<uint32_t> mydataback(bufferSize + 1024);
    size_t recoveredsize = mydataback.size();
    //size_t recoveredsize = bufferSize;


    //spdlog::get("XDBC.CLIENT")->warn("Entered decompress_col, recoveredsize: {0}", recoveredsize);

    //spdlog::get("XDBC.CLIENT")->warn("compressed size {0}", compressed_size);

    codec.decodeArray(reinterpret_cast<const uint32_t *>(src), compressed_size,
                      mydataback.data(), recoveredsize);

    mydataback.resize(recoveredsize);

    //spdlog::get("XDBC.CLIENT")->warn("recovered size {0}, First value: {1}, ", recoveredsize, mydataback[0]);

    //void *ptr = reinterpret_cast<void *>(mydataback.data());

    memcpy(dst, mydataback.data(), recoveredsize * sizeof(uint32_t));

    return 0;
}

static int decompress_fpz(FPZ *fpz, void *data, size_t inbytes) {
    /* read header */
    if (!fpzip_read_header(fpz)) {
        spdlog::get("XDBC.CLIENT")->error("fpzip: cannot read header: {0}", fpzip_errstr[fpzip_errno]);
        return 0;
    }
    /* make sure array size stored in header matches expectations */
    if ((fpz->type == FPZIP_TYPE_FLOAT ? sizeof(float) : sizeof(double)) * fpz->nx * fpz->ny * fpz->nz * fpz->nf !=
        inbytes) {
        fprintf(stderr, "array size does not match dimensions from header\n");
        return 0;
    }
    /* perform actual decompression */
    if (!fpzip_read(fpz, data)) {
        fprintf(stderr, "decompression failed: %s\n", fpzip_errstr[fpzip_errno]);
        return 0;
    }
    return 1;
}

int Decompressor::decompress_double_col(const void *src, size_t compressed_size, void *dst, int bufferSize) {


    auto fpz = fpzip_read_from_buffer(src);

    auto status = decompress_fpz(fpz, dst, bufferSize * sizeof(double));
    fpzip_read_close(fpz);

    //spdlog::get("XDBC.CLIENT")->warn("fpzip status: {0}", status);

    return 0;
}

