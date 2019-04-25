cmake_minimum_required(VERSION 3.5)
project(Cores)
enable_language(CXX)
enable_language(C)
enable_language(ASM)
set(CMAKE_CXX_STANDARD 11)
if (CMAKE_BUILD_TYPE STREQUAL Relese)
    set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -O3 -Wall")
endif ()

include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/third-party/gtest-1.7.0/fused-src)
add_subdirectory(third-party/gtest-1.7.0/fused-src/gtest)

if ((CMAKE_SYSTEM_NAME MATCHES "Linux") OR (CMAKE_SYSTEM_NAME MATCHES "Darwin"))
    set(PTHREAD -lpthread)
    if (CMAKE_SYSTEM_NAME MATCHES "Linux")
        set(AIO /usr/lib64/libaio.so -lrt)
    endif ()
    find_package(Boost 1.50.0 REQUIRED COMPONENTS program_options date_time filesystem iostreams)
    include_directories(${Boost_INCLUDE_DIR})
else ()
    message("Windows cygwin")
    set(CMAKE_C_COMPILER "gcc")
    set(CMAKE_CXX_COMPILER "g++")
    set(BOOST_ROOT D:/cygwin64)
    set(Boost_INCLUDE_DIR ${BOOST_ROOT}/usr/include)
    set(Boost_LIBRARY_DIR ${BOOST_ROOT}/lib)
    find_package(Boost 1.66.0 REQUIRED COMPONENTS program_options date_time filesystem iostreams)
    include_directories(${Boost_INCLUDE_DIR})
    link_directories(${Boost_LIBRARY_DIR})
endif ()

find_package(Threads)

if (WITH_SNAPPY)
    find_package(snappy REQUIRED)
    add_definitions(-DSNAPPY)
    include_directories(${SNAPPY_INCLUDE_DIR})
    list(APPEND THIRDPARTY_LIBS ${SNAPPY_LIBRARIES})
endif ()

#option(WITH_ZLIB "build with zlib" OFF)
if (WITH_ZLIB)
    set(ZLIB_INCLUDE_DIR D:/cygwin64/usr/x86_64-w64-mingw32/sys-root/mingw/include)
    find_package(zlib REQUIRED)
    add_definitions(-DZLIB)
    include_directories(${ZLIB_INCLUDE_DIR})
    list(APPEND THIRDPARTY_LIBS ${ZLIB_LIBRARIES})
endif ()

option(WITH_BZ2 "build with bzip2" OFF)
if (WITH_BZ2)
    find_package(bzip2 REQUIRED)
    add_definitions(-DBZIP2)
    include_directories(${BZIP2_INCLUDE_DIR})
    list(APPEND THIRDPARTY_LIBS ${BZIP2_LIBRARIES})
endif ()

option(WITH_LZ4 "build with lz4" ON)
if (WITH_LZ4)
    if (CMAKE_SYSTEM_NAME MATCHES "Darwin")
        set(LZ4_INCLUDE_DIR /usr/local/include)
        set(LZ4_LIBRARIES /usr/local/lib/liblz4.a)
        include_directories(${LZ4_INCLUDE_DIR})
        add_definitions(-DLZ4)
        list(APPEND THIRDPARTY_LIBS ${LZ4_LIBRARIES})
    elseif (CMAKE_SYSTEM_NAME MATCHES "Linux")
        set(LZ4_DIR /usr)
        #find_package(LZ4 REQUIRED)
        set(LZ4_INCLUDE_DIR ${LZ4_DIR}/include)
        set(LZ4_LIBRARIES ${LZ4_DIR}/lib64/liblz4.so)
        include_directories(${LZ4_INCLUDE_DIR})
        add_definitions(-DLZ4)
        list(APPEND THIRDPARTY_LIBS ${LZ4_LIBRARIES})
    else ()
        set(LZ4_DIR D:/cygwin64/usr/x86_64-w64-mingw32/sys-root/mingw)
        set(LZ4_INCLUDE_DIR D:/cygwin64/usr/x86_64-w64-mingw32/sys-root/mingw/include)
        set(LZ4_LIBRARIES D:/cygwin64/usr/x86_64-w64-mingw32/sys-root/mingw/lib/liblz4.a)
    endif ()
endif ()

if (WITH_ZSTD)
    find_package(zstd REQUIRED)
    add_definitions(-DZSTD)
    include_directories(${ZSTD_INCLUDE_DIR})
    list(APPEND THIRDPARTY_LIBS ${ZSTD_LIBRARIES})
endif ()

include(CheckCXXSourceCompiles)
if (NOT MSVC)
    set(CMAKE_REQUIRED_FLAGS "-msse4.2 -mpclmul")
endif ()
CHECK_CXX_SOURCE_COMPILES("
#include <cstdint>
#include <nmmintrin.h>
#include <wmmintrin.h>
int main() {
  volatile uint32_t x = _mm_crc32_u32(0, 0);
  const auto a = _mm_set_epi64x(0, 0);
  const auto b = _mm_set_epi64x(0, 0);
  const auto c = _mm_clmulepi64_si128(a, b, 0x00);
  auto d = _mm_cvtsi128_si64(c);
}
" HAVE_SSE42)
unset(CMAKE_REQUIRED_FLAGS)
if (HAVE_SSE42)
    add_definitions(-DHAVE_SSE42)
    add_definitions(-DHAVE_PCLMUL)
    set(ISA_CXXFLAG -m32 -mpclmul)
elseif (FORCE_SSE42)
    message(FATAL_ERROR "FORCE_SSE42=ON but unable to compile with SSE4.2 enabled")
endif ()

CHECK_CXX_SOURCE_COMPILES("
#if defined(_MSC_VER) && !defined(__thread)
#define __thread __declspec(thread)
#endif
int main() {
  static __thread int tls;
}
" HAVE_THREAD_LOCAL)
if (HAVE_THREAD_LOCAL)
    add_definitions(-DCORES_SUPPORT_THREAD_LOCAL)
endif ()

include_directories(src)
include_directories(src/io/codec)
include_directories(src/osdep)
include_directories(src/btree)
include_directories(src/bloom)
include_directories(src/block)
include_directories(src/columnar)
include_directories(src/fdtree)
include_directories(src/block/buffer)
include_directories(src/metadata/schema)
include_directories(src/metadata/column)
include_directories(src/metadata/parser)
include_directories(src/metadata/record)
include_directories(src/utils)

set(SOURCE
        src/utils/cJSON.c
        src/utils/CJsonObject.cpp
        src/metadata/parser/EntityDescriptor.cpp)

set(BLOCKSOURCE
        src/block/buffer/BlockCache.cpp)

add_executable(Cores main.cpp ${SOURCE})

set(BTREESOURCE
        src/btree/misc/global.cpp
        src/btree/cache/cache.cpp
        src/btree/BTreeEntry.cpp
        src/btree/BTreeNode.cpp
        src/btree/SerialBTree.cpp)

set(FDTREESOURCE
        src/fdtree/btree.cpp
        src/fdtree/buffer.cpp
        src/fdtree/error.cpp
        src/fdtree/fdtree.cpp
        src/fdtree/lruque.cpp
        src/fdtree/model.cpp
        src/fdtree/pageTable.cpp
        src/fdtree/storage.cpp
        src/fdtree/Timer.cpp src/fdtree/slice.h)

set(BLOOMSOURCE
        src/bloom/bloom_filter/a2.cpp
        src/bloom/bloom_filter/basic.cpp
        src/bloom/bloom_filter/bitwise.cpp
        src/bloom/bloom_filter/counting.cpp
        src/bloom/bloom_filter/stable.cpp
        src/bloom/bitvector.cpp
        src/bloom/counter_vector.cpp
        src/bloom/hash.cpp)

set(BZTREESOURCE
        src/bztree/ebr.cpp
        src/bztree/gc.cpp
        src/bztree/PMwCAS.cpp)

if (WITH_SLOW_BLOOM_TEST)
    set(LSMTREESOURCE
            src/lsmtree/buffer.c
            src/lsmtree/index.c
            src/lsmtree/log.c
            src/lsmtree/skiplist.c
            src/lsmtree/murmur3.c
            src/lsmtree/bloom.c)
endif ()

option(WITH_STDLF_TESTS "build with std lockfree tests" ON)
if (WITH_STDLF_TESTS)
    if (CMAKE_SYSTEM_NAME MATCHES "Darwin")
        set(ATOMICLIBS)
    else ()
        set(ATOMICLIBS -latomic)
    endif ()
    set(STDLF_TESTS
            test/stdlockfree/stdcasTest.cpp)
endif ()

option(WITH_FDTREE_TESTS "build with fdtree tests" ON)
if (WITH_FDTREE_TESTS)
    set(FDTREE_TESTS
            test/btree/FDTreeDbg.cpp
            test/btree/FDTreeTest.cpp
            test/btree/FDBTreeLoad.cpp
            test/btree/FDBTreeSearch.cpp
            test/btree/FDBTreeDbg.cpp
            test/btree/FDBTreeGen.cpp
            )
endif ()

option(WITH_INDEX_TESTS "build with index tests" ON)
if (WITH_INDEX_TESTS)
    set(INDEX_TESTS
            test/btree/btreeTest.cpp
            )
endif ()

option(WITH_BZTREE_TESTS "build with index tests" ON)
if (WITH_BZTREE_TESTS)
    set(BZTREE_TESTS
            test/bztree/bzTreeTest.cpp
            )
endif ()

option(WITH_STANDALONE_TESTS "build with standalone tests" ON)
if (WITH_STANDALONE_TESTS)
    set(STANDALONE_TESTS
            test/primitive/BandwidthTest.cpp
            test/primitive/ParallelBandwidthTest.cpp
            test/primitive/BasicTypeTest.cpp
            test/utils/BitsetTest.cpp
            test/utils/ClearMemory.cpp
            )
endif ()

option(WITH_SIMD_TESTS "build with simd tests" ON)
if (WITH_SIMD_TESTS)
    set(SIMD_TESTS
            test/osdep/MallocAndBeyondTest.cpp
            test/osdep/SIMDBasisTest.cpp
            )
endif ()

option(WITH_BLOCK_TESTS "build with block tests" ON)
if (WITH_BLOCK_TESTS)
    set(BLOCK_TESTS
            test/io/DiskPerformanceTest.cpp
            test/io/DiskAnsyncIOTest.cpp
            test/block/buffer/BlockCacheTest.cpp
            test/block/BlockManagerTest.cpp
            test/block/BlockAppendTest.cpp
            test/io/codec/lz4CodecTest.cpp
            test/io/RandomSeekTest.cpp
            )
endif ()

option(WITH_TESTS "build with tests" ON)
if (WITH_TESTS)
    set(TESTS
            test/metadata/parser/TestTypes.cpp
            test/btree/btreeTestBuild.cpp
            test/btree/btreeTestSearch.cpp
            )
endif ()

option(WITH_LSMTREE_TESTS "build with lsm tests" ON)
if (WITH_LSMTREE_TESTS)
    if (WITH_SLOW_BLOOM_TEST)
        set(LSMTREE_TESTS
                test/lsmtree/LSMBufferTest.c
                test/lsmtree/LSMSkiplistTest.c
                test/lsmtree/LSMIndexTest.c
                test/lsmtree/LSMIndexDbg.c
                test/lsmtree/LSMBloomFilterTest.cpp
                test/lsmtree/BloomTest.c
                )
    else ()
        set(LSMTREE_TESTS
                test/lsmtree/LSMBloomFilterTest.cpp
                )
    endif ()
endif ()

option(WITH_LIBBL_TESTS "build with libbl tests" ON)
if (WITH_LIBBL_TESTS)
    set(LIBBL_TESTS
            test/bloom/LibblBloomFilterBasicTest.cpp
            )
endif ()

enable_testing()
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})

set(SIMD_TEST_EXES ${SIMD_TESTS})
foreach (sourcefile ${SIMD_TEST_EXES})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2 -m64 -mpclmul")
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${LIBS})
    if (NOT "${exename}" MATCHES "db_sanity_test")
        add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
        add_dependencies(check ${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX})
    endif ()
endforeach (sourcefile ${STANDALONE_TEST_EXES})

set(STDLF_TEST_EXES ${STDLF_TESTS})
foreach (sourcefile ${STDLF_TEST_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${ATOMICLIBS})
endforeach (sourcefile ${STDLF_TEST_EXES})

set(STANDALONE_TEST_EXES ${STANDALONE_TESTS})
foreach (sourcefile ${STANDALONE_TEST_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${LIBS} ${PTHREAD})
    if (NOT "${exename}" MATCHES "db_sanity_test")
        add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
        add_dependencies(check ${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX})
    endif ()
endforeach (sourcefile ${STANDALONE_TEST_EXES})

set(BZTREE_TEST_EXES ${BZTREE_TESTS_RUN})
foreach (sourcefile ${BZTREE_TEST_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
endforeach (sourcefile ${BZTREE_TEST_EXES})

set(BLOCK_TEST_EXES ${BLOCK_TESTS})
foreach (sourcefile ${BLOCK_TEST_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${LIBS} ${Boost_LIBRARIES} ${LZ4_LIBRARIES} ${PTHREAD} ${AIO})
    if (NOT "${exename}" MATCHES "db_sanity_test")
        add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
        add_dependencies(check ${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX})
    endif ()
endforeach (sourcefile ${STANDALONE_TEST_EXES})

set(FDTREE_EXES ${FDTREE_TESTS})
foreach (sourcefile ${FDTREE_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${FDTREESOURCE} ${sourcefile})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${LIBS})
    #    if (NOT "${exename}" MATCHES "db_sanity_test")
    #        add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
    #        add_dependencies(check ${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX})
    #    endif ()
endforeach (sourcefile ${FDTREE_EXES})

set(INDEX_EXES ${INDEX_TESTS})
foreach (sourcefile ${INDEX_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${BTREESOURCE} ${sourcefile})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${LIBS})
    if (NOT "${exename}" MATCHES "db_sanity_test")
        add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
        add_dependencies(check ${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX})
    endif ()
endforeach (sourcefile ${INDEX_EXES})

set(TEST_EXES ${TESTS})
foreach (sourcefile ${TEST_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${BTREESOURCE})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX})
    if (NOT "${exename}" MATCHES "db_sanity_test")
        add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
        add_dependencies(check ${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX})
    endif ()
endforeach (sourcefile ${TEST_EXES})

set(LSMTREE_TEST_EXES ${LSMTREE_TESTS})
foreach (sourcefile ${LSMTREE_TEST_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${LSMTREESOURCE} ${sourcefile})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${LIBS})
    if (NOT "${exename}" MATCHES "db_sanity_test")
        add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
        add_dependencies(check ${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX})
    endif ()
endforeach (sourcefile ${LSMTREE_TEST_EXES})

set(LIBBL_TEST_EXES ${LIBBL_TESTS})
foreach (sourcefile ${LIBBL_TEST_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${BLOOMSOURCE} ${sourcefile})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
endforeach (sourcefile ${LIBBL_TEST_EXES})