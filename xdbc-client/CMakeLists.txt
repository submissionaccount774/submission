cmake_minimum_required(VERSION 3.21)

project(xdbc VERSION 0.1 DESCRIPTION "xdbc load")

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -g")
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

#[[
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg")
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pg")
SET(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -pg")
]]

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(spdlog REQUIRED)
find_package(ZLIB REQUIRED)
find_package(FastPFOR REQUIRED)
find_package(FPZIP REQUIRED)
find_library(FPZIP_LIBRARY NAMES fpzip)

#TODO: fix hardcoded paths
set(ZSTD_LIBRARY_PATH "/usr/lib/x86_64-linux-gnu/libzstd.so")
set(SNAPPY_LIBRARY_PATH "/usr/lib/x86_64-linux-gnu/libsnappy.so")

include_directories("/zfp/include")
link_directories(/zfp/lib)

include(GNUInstallDirs)

add_library(xdbc SHARED
        xdbc/xclient.cpp
        xdbc/Decompression/Decompressor.cpp xdbc/Decompression/Decompressor.h)

set_target_properties(xdbc PROPERTIES
        #VERSION ${PROJECT_VERSION}
        #SOVERSION 1
        PUBLIC_HEADER xdbc/xclient.h)

target_include_directories(xdbc PUBLIC .)
target_link_libraries(${PROJECT_NAME} PRIVATE ${ZSTD_LIBRARY_PATH} ${SNAPPY_LIBRARY_PATH} Threads::Threads lzo2 lz4 spdlog::spdlog_header_only ZLIB::ZLIB FastPFOR::FastPFOR ${FPZIP_LIBRARY})
install(TARGETS xdbc
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(FILES xdbc/customQueue.h xdbc/utils.h xdbc/metrics_calculator.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

add_subdirectory(./tests)
