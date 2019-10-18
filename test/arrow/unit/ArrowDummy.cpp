//
// Created by iclab on 10/16/19.
//

//#include "parquet/api/reader.h"
//#include "parquet/api/writer.h"
//#include "parquet/exception.h"
//#include <arrow/io/api.h>
//#include "arrow/writer.h"
//#include "arrow/status.h"
//#include "gtest/gtest.h"
#include <iostream>
#include <cstring>
#include <assert.h>
#include "arrow/api.h"
#include "arrow/array.h"
#include "arrow/type.h"

using namespace std;

//TEST(ArrowTest, WriterTest) {
void write() {
    arrow::Int64Builder i64builder;
    i64builder.AppendValues({1, 2, 3, 4, 5, 6});
    std::shared_ptr<arrow::Array> i64array;
    i64builder.Finish(&i64array);

    arrow::StringBuilder strbuilder;
    strbuilder.AppendValues({"first", "second", "third", "forth", "fifth"});
    std::shared_ptr<arrow::Array> strarray;
    strbuilder.Finish(&strarray);
    std::shared_ptr<arrow::Schema> schema = arrow::schema(
            {arrow::field("int", arrow::int64()), arrow::field("str", arrow::uint8())});
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {i64array, strarray});

    /*std::shared_ptr<arrow::io::FileOutputStream> outfile;
    arrow::io::FileOutputStream::Open("../res/parquet/dummy.par", &outfile);
    parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 3);*/
}

//TEST(ArrowTest, ReaderTest) {
void read() {
//    std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::OpenFile(
//            "../res/parquet/dummy.par");
//    PrintSchema(reader->metadata()->schema()->schema_root().get(), std::cout);
}

int main(int argc, char **argv) {
    write();
    /*::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();*/
}