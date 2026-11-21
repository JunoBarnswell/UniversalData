#ifndef UD_CONNECTOR_ICEBERG_OUTPUT_STREAM_H
#define UD_CONNECTOR_ICEBERG_OUTPUT_STREAM_H

/* Private implementation included by iceberg_table.cpp. */
class NativeOutputStream final : public iceberg::PositionOutputStream {
 public:
    NativeOutputStream(FILE* file, ud_context* context, std::string path)
        : file_(file), context_(context), path_(std::move(path)) {}
    ~NativeOutputStream() override { Close(); }
    Result<int64_t> Position() const override {
        if (!file_) return iceberg::IOError("output stream is closed");
        const int64_t position = ud_tell(file_);
        return position >= 0 ? Result<int64_t>(position)
                             : std::unexpected(iceberg::IOError("output position failed").error());
    }
    Status Write(std::span<const std::byte> data) override {
        if (closed_) return iceberg::IOError("output stream is closed");
        const auto* source = data.data();
        size_t left = data.size();
        while (left) {
            const size_t written = std::fwrite(source, 1, left, file_);
            if (!written) return iceberg::IOError("output write failed");
            source += written;
            left -= written;
        }
        return {};
    }
    Status Flush() override {
        if (file_ && std::fflush(file_) == 0) return {};
        return iceberg::IOError("output flush failed");
    }
    Status Close() override {
        if (closed_) return {};
        closed_ = true;
        FILE* file = file_;
        const ud_status flush_status = ud_flush(context_, file);
        const int close_status = std::fclose(file);
        file_ = nullptr;
        if (flush_status != UD_OK || close_status != 0)
            return iceberg::IOError("output close failed");
        return ud_sync_parent(context_, path_.c_str()) == UD_OK
                   ? Status{} : iceberg::IOError("output directory sync failed");
    }

 private:
    FILE* file_ = nullptr;
    ud_context* context_ = nullptr;
    std::string path_;
    bool closed_ = false;
};

#endif
