#ifndef UD_CONNECTOR_ICEBERG_INPUT_FILE_H
#define UD_CONNECTOR_ICEBERG_INPUT_FILE_H

/* Private implementation included by iceberg_table.cpp. */
class NativeInputFile final : public iceberg::InputFile {
 public:
    using OpenFn = std::function<Result<std::unique_ptr<iceberg::SeekableInputStream>>() >;
    using SizeFn = std::function<Result<int64_t>()>;
    NativeInputFile(std::string location, OpenFn open, SizeFn size)
        : location_(std::move(location)), open_(std::move(open)), size_(std::move(size)) {}
    std::string_view location() const override { return location_; }
    Result<int64_t> Size() const override { return size_(); }
    Result<std::unique_ptr<iceberg::SeekableInputStream>> Open() override { return open_(); }

 private:
    std::string location_;
    OpenFn open_;
    SizeFn size_;
};

#endif
