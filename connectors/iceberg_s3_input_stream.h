#ifndef UD_CONNECTOR_ICEBERG_S3_INPUT_STREAM_H
#define UD_CONNECTOR_ICEBERG_S3_INPUT_STREAM_H

#include "s3_object.h"

/* Private implementation included by iceberg_table.cpp.  Reads are bounded
 * HTTP Range requests so a Parquet scan never materializes an entire object. */
class NativeS3InputStream final : public iceberg::SeekableInputStream {
 public:
    NativeS3InputStream(const ud_component_context* context, ud_connection_lease* lease,
                        std::string location, int64_t size)
        : context_(context), lease_(lease), location_(std::move(location)), size_(size) {}
    ~NativeS3InputStream() override { Close(); }

    Result<int64_t> Position() const override {
        if (closed_) return std::unexpected(iceberg::IOError("S3 input stream is closed").error());
        return position_;
    }
    Status Seek(int64_t position) override {
        if (closed_) return iceberg::IOError("S3 input stream is closed");
        if (position < 0 || position > size_) return iceberg::InvalidArgument("S3 seek outside file");
        position_ = position;
        return {};
    }
    Result<int64_t> Read(std::span<std::byte> out) override {
        if (closed_) return std::unexpected(iceberg::IOError("S3 input stream is closed").error());
        auto read = ReadAt(position_, out);
        if (read.has_value()) position_ += *read;
        return read;
    }
    Status ReadFully(int64_t position, std::span<std::byte> out) override {
        if (closed_) return iceberg::IOError("S3 input stream is closed");
        if (position < 0 || position > size_ || out.size() > static_cast<size_t>(size_ - position))
            return iceberg::InvalidArgument("S3 read outside file");
        auto read = ReadAt(position, out);
        if (!read.has_value()) return std::unexpected(read.error());
        return static_cast<uint64_t>(*read) == out.size() ? Status{} : iceberg::IOError("S3 range read was short");
    }
    Status Close() override {
        closed_ = true;
        return {};
    }

 private:
    Result<int64_t> ReadAt(int64_t position, std::span<std::byte> out) const {
        const uint64_t available = static_cast<uint64_t>(size_ - position);
        const uint64_t wanted = std::min<uint64_t>(available, out.size());
        uint64_t total = 0;
        while (total < wanted) {
            const uint64_t count = std::min<uint64_t>(wanted - total, 8 * 1024 * 1024);
            uint64_t received = 0;
            const ud_status status = ud_s3_object_read(
                context_, lease_, {location_.data(), static_cast<uint32_t>(location_.size())},
                static_cast<uint64_t>(position) + total,
                reinterpret_cast<uint8_t*>(out.data()) + total, count, &received);
            if (status) return std::unexpected(iceberg::IOError("S3 range read failed").error());
            if (!received) break;
            total += received;
            if (received < count) break;
        }
        return static_cast<int64_t>(total);
    }
    const ud_component_context* context_ = nullptr;
    ud_connection_lease* lease_ = nullptr;
    std::string location_;
    int64_t size_ = 0;
    int64_t position_ = 0;
    bool closed_ = false;
};

#endif
