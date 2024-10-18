cmake_minimum_required(VERSION 3.21)

project(testxdbc VERSION 0.1 DESCRIPTION "test xdbc")

set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -g")
#[[SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg")
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pg")
SET(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -pg")]]

add_executable(test_xclient test_xclient.cpp Tester.cpp Tester.h)

find_package(spdlog REQUIRED)

find_package(Boost 1.40.0 REQUIRED system program_options)
IF (Boost_FOUND)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS})
    LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})
ENDIF (Boost_FOUND)

SET(USED_LIBS ${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY} ${Boost_REGEX_LIBRARY})

target_link_libraries(test_xclient PRIVATE xdbc spdlog::spdlog ${USED_LIBS})