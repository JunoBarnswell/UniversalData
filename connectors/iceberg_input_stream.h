#ifndef UD_CONNECTOR_ICEBERG_INPUT_STREAM_H
#define UD_CONNECTOR_ICEBERG_INPUT_STREAM_H

/* Private implementation included by iceberg_table.cpp. */
class NativeInputStream final : public iceberg::SeekableInputStream {
 public:
    NativeInputStream(FILE* file, int64_t size) : file_(file), size_(size) {}
    ~NativeInputStream() override { Close(); }
    Result<int64_t> Position() const override {
        if (!file_) return iceberg::IOError("input stream is closed");
        const int64_t position = ud_tell(file_);
        return position >= 0 ? Result<int64_t>(position)
                             : std::unexpected(iceberg::IOError("input position failed").error());
    }
    Status Seek(int64_t position) override {
        if (position < 0 || static_cast<uint64_t>(position) > static_cast<uint64_t>(size_))
            return iceberg::InvalidArgument("input seek outside file");
        if (!file_ || ud_seek(file_, static_cast<uint64_t>(position)) != 0)
            return iceberg::IOError("input seek failed");
        return {};
    }
    Result<int64_t> Read(std::span<std::byte> out) override {
        if (!file_) return std::unexpected(iceberg::IOError("input stream is closed").error());
        const size_t count = out.empty() ? 0 : std::fread(out.data(), 1, out.size(), file_);
        if (count != out.size() && std::ferror(file_))
            return std::unexpected(iceberg::IOError("input read failed").error());
        return static_cast<int64_t>(count);
    }
    Status ReadFully(int64_t position, std::span<std::byte> out) override {
        const uint64_t size = static_cast<uint64_t>(size_);
        if (position < 0 || static_cast<uint64_t>(position) > size ||
            out.size() > size - static_cast<size_t>(position))
            return iceberg::IOError("input read outside file");
        if (!file_ || ud_seek(file_, static_cast<uint64_t>(position)) != 0)
            return iceberg::IOError("input seek failed");
        if (!out.empty() && std::fread(out.data(), 1, out.size(), file_) != out.size())
            return iceberg::IOError("input read failed");
        return {};
    }
    Status Close() override {
        if (!file_) return {};
        FILE* file = file_;
        file_ = nullptr;
        return std::fclose(file) == 0 ? Status{}
                                      : iceberg::IOError("input close failed");
    }

 private:
    FILE* file_ = nullptr;
    int64_t size_ = 0;
};

#endif
