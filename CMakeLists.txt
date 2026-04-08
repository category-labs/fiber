# Copyright 2020, 2021 Peter Dimov
# Distributed under the Boost Software License, Version 1.0.
# https://www.boost.org/LICENSE_1_0.txt

cmake_minimum_required(VERSION 3.8...3.20)

project(boost_fiber VERSION "${BOOST_SUPERPROJECT_VERSION}" LANGUAGES CXX)

# boost_fiber

add_library(boost_fiber
  src/algo/algorithm.cpp
  src/algo/round_robin.cpp
  src/condition_variable.cpp
  src/context.cpp
  src/fiber.cpp
  src/future.cpp
  src/mutex.cpp
  src/properties.cpp
  src/scheduler.cpp
  src/waker.cpp
)

add_library(Boost::fiber ALIAS boost_fiber)

target_include_directories(boost_fiber PUBLIC include)

target_link_libraries(boost_fiber
  PUBLIC
    Boost::assert
    Boost::config
    Boost::context
    Boost::core
    Boost::intrusive
    Boost::predef
    Boost::smart_ptr
)

target_compile_features(boost_fiber PUBLIC cxx_std_11)

target_compile_definitions(boost_fiber
  PUBLIC BOOST_FIBER_NO_LIB
  PRIVATE BOOST_FIBER_SOURCE BOOST_FIBERS_SOURCE
)

if(BUILD_SHARED_LIBS)
  target_compile_definitions(boost_fiber PUBLIC BOOST_FIBER_DYN_LINK BOOST_FIBERS_DYN_LINK)
else()
  target_compile_definitions(boost_fiber PUBLIC BOOST_FIBER_STATIC_LINK)
endif()

# Install

if(BOOST_SUPERPROJECT_VERSION AND NOT CMAKE_VERSION VERSION_LESS 3.13)
  boost_install(TARGETS boost_fiber VERSION ${BOOST_SUPERPROJECT_VERSION} HEADER_DIRECTORY include)
endif()

# Test

if(BUILD_TESTING AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test/CMakeLists.txt")

  add_subdirectory(test)

endif()
