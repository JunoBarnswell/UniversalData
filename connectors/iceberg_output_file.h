#ifndef UD_CONNECTOR_ICEBERG_OUTPUT_FILE_H
#define UD_CONNECTOR_ICEBERG_OUTPUT_FILE_H

/* Private implementation included by iceberg_table.cpp. */
class NativeOutputFile final : public iceberg::OutputFile {
 public:
    using OpenFn = std::function<Result<std::unique_ptr<iceberg::PositionOutputStream>>() >;
    NativeOutputFile(std::string location, OpenFn create, OpenFn overwrite)
        : location_(std::move(location)), create_(std::move(create)), overwrite_(std::move(overwrite)) {}
    std::string_view location() const override { return location_; }
    Result<std::unique_ptr<iceberg::PositionOutputStream>> Create() override { return create_(); }
    Result<std::unique_ptr<iceberg::PositionOutputStream>> CreateOrOverwrite() override { return overwrite_(); }

 private:
    std::string location_;
    OpenFn create_;
    OpenFn overwrite_;
};

#endif
