#ifndef UD_CONNECTOR_ICEBERG_FILE_IO_H
#define UD_CONNECTOR_ICEBERG_FILE_IO_H

/* Private implementation included by iceberg_table.cpp. */
class NativeFileIO final : public iceberg::FileIO {
 public:
    explicit NativeFileIO(const ud_iceberg_options& options)
        : options_(options), storage_root_(trim_slash(copy_string(options.storage_root))) {}

    Result<std::unique_ptr<iceberg::InputFile>> NewInputFile(std::string location) override {
        auto authorized = Authorize(location, UD_ACCESS_READ, false, nullptr);
        if (!authorized.has_value()) return std::unexpected(authorized.error());
        auto open = [this, location] { return OpenInput(location); };
        auto size = [this, location] { return SizeInput(location); };
        return std::unique_ptr<iceberg::InputFile>(
            new NativeInputFile(std::move(location), std::move(open), std::move(size)));
    }
    Result<std::unique_ptr<iceberg::InputFile>> NewInputFile(std::string location,
                                                               size_t) override {
        return NewInputFile(std::move(location));
    }
    Result<std::unique_ptr<iceberg::OutputFile>> NewOutputFile(std::string location) override {
        auto authorized = Authorize(location, UD_ACCESS_WRITE, true, nullptr);
        if (!authorized.has_value()) return std::unexpected(authorized.error());
        auto create = [this, location] { return OpenOutput(location, false); };
        auto overwrite = [this, location] { return OpenOutput(location, true); };
        return std::unique_ptr<iceberg::OutputFile>(
            new NativeOutputFile(std::move(location), std::move(create), std::move(overwrite)));
    }
    Result<std::string> ReadFile(const std::string& location,
                                 std::optional<size_t> length) override {
        auto input = OpenInput(location);
        if (!input.has_value()) return std::unexpected(input.error());
        auto size = SizeInput(location);
        if (!size.has_value()) return std::unexpected(size.error());
        if (*size < 0 || static_cast<uint64_t>(*size) > SIZE_MAX)
            return std::unexpected(iceberg::IOError("Iceberg file is too large").error());
        const size_t count = length ? std::min(*length, static_cast<size_t>(*size))
                                    : static_cast<size_t>(*size);
        std::string bytes(count, '\0');
        if (count) {
            auto status = (*input)->ReadFully(0, {reinterpret_cast<std::byte*>(bytes.data()), count});
            if (!status.has_value()) return std::unexpected(status.error());
        }
        (*input)->Close();
        return bytes;
    }
    Status WriteFile(const std::string& location, std::string_view bytes) override {
        auto output = OpenOutput(location, true);
        if (!output.has_value()) return std::unexpected(output.error());
        auto status = (*output)->Write({reinterpret_cast<const std::byte*>(bytes.data()), bytes.size()});
        if (!status.has_value()) return status;
        return (*output)->Close();
    }
    Status DeleteFile(const std::string& location) override {
        std::string path;
        auto authorized = Authorize(location, UD_ACCESS_DELETE, false, &path);
        if (!authorized.has_value()) return std::unexpected(authorized.error());
        const ud_status status = ud_remove_file(options_.memory, path.c_str());
        return status == UD_OK ? Status{} : iceberg::IOError("Iceberg file delete failed");
    }

    Status AuthorizeLocation(std::string_view location, int32_t action) const {
        return Authorize(location, action, action == UD_ACCESS_WRITE, nullptr);
    }

 private:
    static std::string ComparePath(const std::filesystem::path& path) {
        std::string result = path.lexically_normal().string();
        for (char& c : result) if (c == '\\') c = '/';
        return trim_slash(std::move(result));
    }
    Status Resolve(std::string_view location, std::string& actual) const {
        const std::string root_string = uri_path(storage_root_);
        if (root_string.empty()) return iceberg::NotAuthorized("Iceberg storage root is missing");
        std::error_code ec;
        std::filesystem::path root = std::filesystem::absolute(root_string, ec);
        if (ec) return iceberg::NotAuthorized("Iceberg storage root is invalid");
        root = root.lexically_normal();
        std::filesystem::path candidate = std::filesystem::absolute(uri_path(location), ec);
        if (ec) return iceberg::NotAuthorized("Iceberg file path is invalid");
        candidate = candidate.lexically_normal();
        if (!path_has_prefix(ComparePath(candidate), ComparePath(root)))
            return iceberg::NotAuthorized("Iceberg file is outside the configured storage root");
        std::filesystem::path existing = candidate;
        while (true) {
            const auto state = std::filesystem::symlink_status(existing, ec);
            if (ec && ec != std::errc::no_such_file_or_directory)
                return iceberg::IOError(ec.message());
            if (!ec && state.type() != std::filesystem::file_type::not_found) break;
            ec.clear();
            const auto parent = existing.parent_path();
            if (parent == existing) return iceberg::IOError("Iceberg file parent is missing");
            existing = parent;
        }
        if (ud_path_no_symlinks(options_.memory, existing.string().c_str()) != UD_OK)
            return iceberg::NotAuthorized("Iceberg file path contains a link or non-directory");
        const auto canonical_root = std::filesystem::weakly_canonical(root, ec);
        if (ec) return iceberg::NotAuthorized("Iceberg storage root cannot be resolved");
        const auto canonical_existing = std::filesystem::weakly_canonical(existing, ec);
        if (ec) return iceberg::NotAuthorized("Iceberg file parent cannot be resolved");
        const auto relative = candidate.lexically_relative(existing);
        const auto resolved = relative == "." ? canonical_existing
                                               : (canonical_existing / relative).lexically_normal();
        if (!path_has_prefix(ComparePath(resolved), ComparePath(canonical_root)))
            return iceberg::NotAuthorized("Iceberg file resolves outside the configured storage root");
        actual = resolved.string();
        return {};
    }
    Status Authorize(std::string_view location, int32_t action, bool create_parent,
                     std::string* actual) const {
        if (remote_uri(location) || (has_uri_scheme(location) && !starts_with(location, "file:")))
            return iceberg::NotSupported("Iceberg FileIO only supports local file locations");
        std::string resolved;
        auto status = Resolve(location, resolved);
        if (!status.has_value()) return status;
        if (!options_.authorize_file) return iceberg::NotAuthorized("Iceberg file grant is missing");
        const auto granted = [&] {
            ud_string path{resolved.data(), static_cast<uint32_t>(resolved.size())};
            return options_.authorize_file(options_.user, path, action) == UD_OK;
        };
        if (!granted())
            return iceberg::NotAuthorized("Iceberg file grant denied");
        if (create_parent) {
            std::error_code ec;
            std::filesystem::create_directories(std::filesystem::path(resolved).parent_path(), ec);
            if (ec) return iceberg::IOError(ec.message());
            status = Resolve(location, resolved);
            if (!status.has_value()) return status;
            if (!granted()) return iceberg::NotAuthorized("Iceberg file grant denied");
        }
        if (actual) *actual = std::move(resolved);
        return {};
    }
    static int SeekEnd(FILE* file) {
#ifdef _WIN32
        return _fseeki64(file, 0, SEEK_END);
#else
        return fseeko(file, 0, SEEK_END);
#endif
    }
    static Result<int64_t> FileSize(FILE* file) {
        if (SeekEnd(file) != 0) return std::unexpected(iceberg::IOError("Iceberg file size failed").error());
        const int64_t size = ud_tell(file);
        if (size < 0 || ud_seek(file, 0) != 0)
            return std::unexpected(iceberg::IOError("Iceberg file size failed").error());
        return size;
    }
    Result<std::unique_ptr<iceberg::SeekableInputStream>> OpenInput(std::string_view location) const {
        std::string path;
        auto authorized = Authorize(location, UD_ACCESS_READ, false, &path);
        if (!authorized.has_value()) return std::unexpected(authorized.error());
        FILE* file = nullptr;
        if (ud_open_regular(options_.memory, path.c_str(), &file) != UD_OK || !file)
            return std::unexpected(iceberg::IOError("Iceberg file open failed").error());
        auto size = FileSize(file);
        if (!size.has_value()) {
            std::fclose(file);
            return std::unexpected(size.error());
        }
        return std::unique_ptr<iceberg::SeekableInputStream>(new NativeInputStream(file, *size));
    }
    Result<int64_t> SizeInput(std::string_view location) const {
        std::string path;
        auto authorized = Authorize(location, UD_ACCESS_READ, false, &path);
        if (!authorized.has_value()) return std::unexpected(authorized.error());
        FILE* file = nullptr;
        if (ud_open_regular(options_.memory, path.c_str(), &file) != UD_OK || !file)
            return std::unexpected(iceberg::IOError("Iceberg file open failed").error());
        auto size = FileSize(file);
        std::fclose(file);
        return size;
    }
    static FILE* OpenWritable(std::string_view path, bool overwrite) {
#ifndef _WIN32
        int flags = O_WRONLY | O_CREAT | (overwrite ? O_TRUNC : O_EXCL);
#ifdef O_CLOEXEC
        flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
        flags |= O_NOFOLLOW;
#endif
        const int descriptor = open(std::string(path).c_str(), flags, 0600);
        if (descriptor < 0) return nullptr;
        FILE* file = fdopen(descriptor, "wb");
        if (!file) close(descriptor);
        return file;
#else
        return ud_fopen(std::string(path).c_str(), overwrite ? "wb" : "wbx");
#endif
    }
    Result<std::unique_ptr<iceberg::PositionOutputStream>> OpenOutput(std::string_view location,
                                                                       bool overwrite) const {
        std::string path;
        auto authorized = Authorize(location, UD_ACCESS_WRITE, true, &path);
        if (!authorized.has_value()) return std::unexpected(authorized.error());
        FILE* file = OpenWritable(path, overwrite);
        if (!file) {
            if (!overwrite && errno == EEXIST)
                return std::unexpected(iceberg::AlreadyExists("Iceberg output exists").error());
            return std::unexpected(iceberg::IOError("Iceberg output open failed").error());
        }
        return std::unique_ptr<iceberg::PositionOutputStream>(
            new NativeOutputStream(file, options_.memory, std::move(path)));
    }
    ud_iceberg_options options_;
    std::string storage_root_;
};

#endif
