cmake_minimum_required(VERSION 3.5)
project(BW)

set(SUB_PROJECT_NAME "BW")

include_directories(../../src/utils)
include_directories(../../third-party/gtest-1.7.0/fused-src)
include_directories(.)
include_directories(src)

set(EXT_LIBS
        ../../src/utils/utils.cpp)

set(BW_LIBS
        src/bitvector.cpp
        src/bitvector_block.cpp
        src/bitvector_iterator.cpp
        src/bwh_column_block.cpp
        src/bwh_column_iterator.cpp
        src/bwv_column_block.cpp
        src/bwv_column_iterator.cpp
        src/column.cpp
        src/column_iterator.cpp
        src/env.cpp
        src/file.cpp
        src/iterator.cpp
        src/naive_column_block.cpp
        src/options.cpp
        src/status.cpp
        src/table.cpp
        src/utility.cpp)

set(BW_TESTS
        test/dummy.cpp
        )

message("Subproject")

set(BW_TESTS_EXES ${BW_TESTS})
foreach (sourcefile ${BW_TESTS_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${SUB_PROJECT_NAME}_${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${BW_LIBS})
    set_target_properties(${SUB_PROJECT_NAME}_${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${SUB_PROJECT_NAME}_${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest)
    if (NOT "${exename}" MATCHES "db_sanity_test")
        add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
        #add_dependencies(check ${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX})
    endif ()
endforeach (sourcefile ${BW_TESTS_EXES})