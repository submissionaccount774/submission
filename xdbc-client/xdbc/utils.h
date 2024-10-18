#ifndef UTILS_H
#define UTILS_H

#include <iostream>
#include <boost/crc.hpp>

class Utils {
public:
    struct shortLineitem {
        int l_orderkey;
        int l_partkey;
        int l_suppkey;
        int l_linenumber;
        double l_quantity;
        double l_extendedprice;
        double l_discount;
        double l_tax;
    };

    static std::string boolVectorToString(const std::vector<std::atomic<bool>> &vec) {
        std::string result;
        for (size_t i = 0; i < vec.size(); ++i) {
            result += vec[i] ? "1" : "0";
            if (i != vec.size() - 1) {
                result += ",";
            }
        }
        return result;
    }

    static uint16_t compute_checksum(const uint8_t *data, std::size_t size) {
        uint16_t checksum = 0;
        for (std::size_t i = 0; i < size; ++i) {
            checksum ^= data[i];
        }
        return checksum;
    }

    static size_t compute_crc(const boost::asio::const_buffer &buffer) {
        boost::crc_32_type crc;
        crc.process_bytes(boost::asio::buffer_cast<const void *>(buffer), boost::asio::buffer_size(buffer));
        return crc.checksum();
    }


    static std::string boolVecAsStr(std::vector<std::atomic<bool>> &vec) {
        std::string result;

        for (size_t i = 0; i < vec.size(); ++i) {
            result += vec[i].load() ? "1" : "0";

            if (i < vec.size() - 1) {
                result += ",";
            }
        }

        return result;
    }

    static void printSl(shortLineitem *t) {
        std::cout << t->l_orderkey << " | "
                  << t->l_partkey << " | "
                  << t->l_suppkey << " | "
                  << t->l_linenumber << " | "
                  << t->l_quantity << " | "
                  << t->l_extendedprice << " | "
                  << t->l_discount << " | "
                  << t->l_tax
                  << std::endl;
    }

    static std::string slStr(shortLineitem *t) {

        return std::to_string(t->l_orderkey) + std::string(", ") +
               std::to_string(t->l_partkey) + std::string(", ") +
               std::to_string(t->l_suppkey) + std::string(", ") +
               std::to_string(t->l_linenumber) + std::string(", ") +
               std::to_string(t->l_quantity) + std::string(", ") +
               std::to_string(t->l_extendedprice) + std::string(", ") +
               std::to_string(t->l_discount) + std::string(", ") +
               std::to_string(t->l_tax);
    }
};

#endif // UTILS_H