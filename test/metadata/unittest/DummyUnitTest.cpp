//
// Created by iclab on 12/6/19.
//

#include <gtest/gtest.h>

void SETUP() {

}

TEST(SchemaTest, DummyTest) {
}

void TEARDOWN() {

}

int main(int argc, char **argv) {
    SETUP();
    ::testing::InitGoogleTest(&argc, argv);
    int ret = RUN_ALL_TESTS();
    TEARDOWN();
    return ret;
}