//
// Created by Michael on 2019-10-11.
//

#include <atomic>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <thread>
#include "gtest/gtest.h"

TEST(MaskRWPtr, UninRWTest) {
    uint64_t ul = 0;
    std::cout << ul << std::endl;
    std::atomic<uint64_t> aul(ul);
    aul.store(1);
    std::cout << aul << ":" << ul << std::endl;
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}