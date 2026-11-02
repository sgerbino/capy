#
# Copyright (c) 2026 Steve Gerbino
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
#
# Official repository: https://github.com/cppalliance/capy
#

# Compile-only: the launchers need exceptions, so nothing here can run.
add_library(boost_capy_tests_no_exceptions OBJECT quitter.cpp)
target_link_libraries(boost_capy_tests_no_exceptions PRIVATE Boost::capy)

if(MSVC)
    target_compile_options(boost_capy_tests_no_exceptions PRIVATE /EHs-c-)
    target_compile_definitions(boost_capy_tests_no_exceptions PRIVATE _HAS_EXCEPTIONS=0)
else()
    target_compile_options(boost_capy_tests_no_exceptions PRIVATE -fno-exceptions)
endif()

add_dependencies(tests boost_capy_tests_no_exceptions)
