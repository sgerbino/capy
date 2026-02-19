# Installing Boost.Capy

Capy has no external dependencies beyond a C++20 compiler and CMake. It
can be consumed by external CMake projects in two ways.

## Install and find_package

Build, install to a prefix, then consume with `find_package` from any
project.

```bash
git clone https://github.com/cppalliance/capy.git
cd capy
cmake -B _build -DCMAKE_BUILD_TYPE=Release
cmake --build _build
cmake --install _build --prefix /path/to/prefix
```

In your project:

```cmake
cmake_minimum_required(VERSION 3.20)
project(myapp CXX)

find_package(boost_capy REQUIRED)

add_executable(myapp main.cpp)
target_link_libraries(myapp PRIVATE Boost::capy)
```

Configure with the install prefix:

```bash
cmake -B _build -DCMAKE_PREFIX_PATH=/path/to/prefix
cmake --build _build
```

## FetchContent

Pull Capy directly into your CMake build.

```cmake
cmake_minimum_required(VERSION 3.20)
project(myapp CXX)

include(FetchContent)
FetchContent_Declare(
    capy
    GIT_REPOSITORY https://github.com/cppalliance/capy.git
    GIT_TAG develop
    GIT_SHALLOW TRUE)

set(BOOST_CAPY_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(BOOST_CAPY_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BOOST_CAPY_BUILD_BENCH OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(capy)

add_executable(myapp main.cpp)
target_link_libraries(myapp PRIVATE Boost::capy)
```

## CMake Targets

| Target | Description |
|---|---|
| `Boost::capy` | Core library (buffers, executors, coroutine types) |

## Requirements

- CMake 3.20 or later
- C++20 compiler (GCC 12+, Clang 17+, MSVC 14.34+)
- Ninja (recommended) or other CMake generator
