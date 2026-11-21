#ifndef UD_CONNECTOR_ICEBERG_S3_OUTPUT_STREAM_H
#define UD_CONNECTOR_ICEBERG_S3_OUTPUT_STREAM_H

#include "s3_object.h"

class NativeS3OutputStream final : public iceberg::PositionOutputStream {
 public:
    NativeS3OutputStream(const ud_component_context* context, ud_connection_lease* lease,
                          ud_buffer_manager* buffers, std::string location, bool overwrite)
        : context_(context), lease_(lease), buffers_(buffers), location_(std::move(location)), overwrite_(overwrite),
          file_(std::tmpfile()) {}
    ~NativeS3OutputStream() override {
        if (file_) std::fclose(file_);
        if (reserved_) ud_buffer_release_external(buffers_, reserved_);
    }

    bool valid() const { return file_ != nullptr; }
    Result<int64_t> Position() const override {
        if (closed_ || !file_)
            return std::unexpected(iceberg::IOError("S3 output stream is closed").error());
        const int64_t position = ud_tell(file_);
        return position >= 0 ? Result<int64_t>(position)
                             : std::unexpected(iceberg::IOError("S3 output position failed").error());
    }
    Status Write(std::span<const std::byte> data) override {
        if (closed_ || !file_) return iceberg::IOError("S3 output stream is closed");
        if (data.empty()) return {};
        const ud_status reserved = ud_buffer_reserve_external(buffers_, data.size());
        if (reserved) {
            const char* message = "S3 output staging exceeds the Native disk quota";
            ud_context_report(context_->memory, reserved, {message, static_cast<uint32_t>(std::strlen(message))});
            return iceberg::IOError(message);
        }
        reserved_ += data.size();
        const auto* source = data.data();
        size_t left = data.size();
        while (left) {
            const size_t written = std::fwrite(source, 1, left, file_);
            if (!written) return iceberg::IOError("S3 output write failed");
            source += written;
            left -= written;
        }
        return {};
    }
    Status Flush() override {
        if (file_ && std::fflush(file_) == 0) return {};
        return iceberg::IOError("S3 output flush failed");
    }
    Status Close() override {
        if (closed_) return {};
        closed_ = true;
        if (!file_) return iceberg::IOError("S3 output stage is unavailable");
        const ud_status flush = std::fflush(file_) == 0 ? UD_OK : UD_IO;
        char digest[65] = {0};
        ud_status upload = flush;
        if (!upload)
            upload = ud_s3_object_upload(
                context_, lease_, {location_.data(), static_cast<uint32_t>(location_.size())}, file_,
                overwrite_, digest);
        const int close = std::fclose(file_);
        file_ = nullptr;
        ud_buffer_release_external(buffers_, reserved_);
        reserved_ = 0;
        if (upload == UD_FENCED)
            return iceberg::AlreadyExists("S3 object already exists");
        if (upload == UD_AUTH)
            return iceberg::NotAuthorized("S3 object upload was denied");
        if (upload == UD_COMMIT_UNKNOWN)
            return iceberg::CommitStateUnknown("S3 object upload outcome is unknown");
        if (upload || close != 0)
            return iceberg::IOError("S3 object upload failed");
        return {};
    }

 private:
    const ud_component_context* context_ = nullptr;
    ud_connection_lease* lease_ = nullptr;
    ud_buffer_manager* buffers_ = nullptr;
    std::string location_;
    bool overwrite_ = false;
    FILE* file_ = nullptr;
    uint64_t reserved_ = 0;
    bool closed_ = false;
};

#endif
