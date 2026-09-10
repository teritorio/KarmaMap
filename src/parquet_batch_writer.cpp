#include "parquet_batch_writer.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <stdexcept>
#include <utility>

#include "h3_utils.hpp"

namespace parquet_out {

namespace {

std::shared_ptr<arrow::Schema> make_schema() {
    return arrow::schema({
        arrow::field("h3_cell", arrow::uint64(), /*nullable=*/false),
        arrow::field("change_date", arrow::uint16(), /*nullable=*/false),
        arrow::field("count", arrow::uint32(), /*nullable=*/false),
    });
}

}  // namespace

struct ParquetBatchWriter::Impl {
    std::shared_ptr<arrow::io::FileOutputStream> out_stream;
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    std::shared_ptr<arrow::Schema> schema;
    bool closed = false;
};

ParquetBatchWriter::ParquetBatchWriter(const std::string& output_path)
    : impl_(std::make_unique<Impl>()) {
    impl_->schema = make_schema();

    auto stream_result = arrow::io::FileOutputStream::Open(output_path);
    if (!stream_result.ok()) {
        throw std::runtime_error("Failed to open Parquet output file: " +
                                  stream_result.status().ToString());
    }
    impl_->out_stream = *stream_result;

    parquet::WriterProperties::Builder props_builder;
    props_builder.compression(parquet::Compression::ZSTD);
    auto writer_props = props_builder.build();

    auto writer_result = parquet::arrow::FileWriter::Open(
        *impl_->schema, arrow::default_memory_pool(), impl_->out_stream, writer_props);
    if (!writer_result.ok()) {
        throw std::runtime_error("Failed to create Parquet FileWriter: " +
                                  writer_result.status().ToString());
    }
    impl_->writer = std::move(*writer_result);
}

ParquetBatchWriter::~ParquetBatchWriter() {
    if (!impl_->closed) close();
}

void ParquetBatchWriter::flush(CountMap& counts) {
    if (counts.empty()) return;

    arrow::UInt64Builder cell_builder;
    arrow::UInt16Builder date_builder;
    arrow::UInt32Builder count_builder;

    if (!cell_builder.Reserve(counts.size()).ok() || !date_builder.Reserve(counts.size()).ok() ||
        !count_builder.Reserve(counts.size()).ok()) {
        throw std::runtime_error("Reserve() failed during Parquet flush");
    }

    for (const auto& [key, count] : counts) {
        auto s1 = cell_builder.Append(key.h3_cell);
        auto s2 = date_builder.Append(h3_utils::require_u16_day(key.day));
        auto s3 = count_builder.Append(count);
        if (!s1.ok() || !s2.ok() || !s3.ok()) {
            throw std::runtime_error("Failed to append a row to the Parquet batch");
        }
    }

    std::shared_ptr<arrow::Array> cell_array, date_array, count_array;
    auto f1 = cell_builder.Finish(&cell_array);
    auto f2 = date_builder.Finish(&date_array);
    auto f3 = count_builder.Finish(&count_array);
    if (!f1.ok() || !f2.ok() || !f3.ok()) {
        throw std::runtime_error("Failed to finalize Arrow columns");
    }

    auto table = arrow::Table::Make(impl_->schema, {cell_array, date_array, count_array});

    // Successive WriteTable() calls append row groups to the same file, so the
    // whole dataset is never held in memory at once.
    auto write_status = impl_->writer->WriteTable(*table, /*chunk_size=*/counts.size());
    if (!write_status.ok()) {
        throw std::runtime_error("Failed to write Parquet row group: " +
                                  write_status.ToString());
    }

    counts.clear();
}

void ParquetBatchWriter::close() {
    if (impl_->closed) return;
    auto status = impl_->writer->Close();
    if (!status.ok()) {
        throw std::runtime_error("Failed to close Parquet writer: " + status.ToString());
    }
    impl_->closed = true;
}

}  // namespace parquet_out
