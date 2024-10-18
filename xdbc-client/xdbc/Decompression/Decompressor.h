#ifndef XDBC_DECOMPRESSOR_H
#define XDBC_DECOMPRESSOR_H

#include <boost/asio.hpp>

class Decompressor {
public:

    static int decompress(int method, void *dst, const void *src, size_t in_size, int out_size);

    static int decompress_zstd(void *dst, const void *src, size_t in_size, int out_size);

    static int decompress_snappy(void *dst, const void *src, size_t in_size, int out_size);

    static int decompress_lzo(void *dst, const void *src, size_t in_size, int out_size);

    static int decompress_lz4(void *dst, const void *src, size_t in_size, int out_size);

    static int decompress_zlib(void *dst, const void *src, size_t in_size, int out_size);

    static int decompress_double_col(const void *src, size_t compressed_size, void *dst, int bufferSize);

    static int decompress_int_col(const void *src, size_t compressed_size, void *dst, int bufferSize);
};

#endif //XDBC_DECOMPRESSOR_H
