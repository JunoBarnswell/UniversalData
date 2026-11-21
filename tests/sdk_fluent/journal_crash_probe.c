/* Linux test-only crash injection at the Native transaction decision's durable publication. */
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

static uint64_t little_endian(const unsigned char *value) {
    uint64_t result = 0;
    for (unsigned index = 0; index < 8; ++index)
        result |= (uint64_t)value[index] << (8 * index);
    return result;
}

static int read_file(int directory, const char *name, unsigned char **output, size_t *length) {
    *output = NULL;
    *length = 0;
    int file = openat(directory, name, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (file < 0)
        return 0;
    struct stat info;
    if (fstat(file, &info) || info.st_size < 0 || (uintmax_t)info.st_size > UINTMAX_C(67108864)) {
        close(file);
        return 0;
    }
    size_t size = (size_t)info.st_size;
    unsigned char *data = malloc(size ? size : 1);
    if (!data) {
        close(file);
        return 0;
    }
    size_t at = 0;
    while (at < size) {
        ssize_t count = read(file, data + at, size - at);
        if (count < 0 && errno == EINTR)
            continue;
        if (count <= 0) {
            free(data);
            close(file);
            return 0;
        }
        at += (size_t)count;
    }
    int failed = close(file);
    if (failed) {
        free(data);
        return 0;
    }
    *output = data;
    *length = size;
    return 1;
}

static int segment_number(const char *name, uint64_t *first) {
    static const char prefix[] = "segment-";
    static const char suffix[] = ".udg";
    size_t length = strlen(name);
    if (length != sizeof(prefix) - 1 + 20 + sizeof(suffix) - 1 ||
        memcmp(name, prefix, sizeof(prefix) - 1) ||
        memcmp(name + sizeof(prefix) - 1 + 20, suffix, sizeof(suffix) - 1))
        return 0;
    uint64_t value = 0;
    for (size_t at = sizeof(prefix) - 1; at < sizeof(prefix) - 1 + 20; ++at) {
        if (name[at] < '0' || name[at] > '9')
            return 0;
        value = value * 10 + (uint64_t)(name[at] - '0');
    }
    if (!value)
        return 0;
    *first = value;
    return 1;
}

static int commit_record(const unsigned char *records, size_t length) {
    size_t at = 0;
    while (at < length) {
        if (length - at < 8)
            return 0;
        uint64_t kind = little_endian(records + at);
        at += 8;
        uint64_t key = 0;
        if (length - at < 8)
            return 0;
        key = little_endian(records + at);
        at += 8;
        if (key > length - at)
            return 0;
        at += (size_t)key;
        uint64_t owner = 0;
        if (length - at < 8)
            return 0;
        owner = little_endian(records + at);
        at += 8;
        if (owner > length - at)
            return 0;
        at += (size_t)owner;
        uint64_t payload = 0;
        if (length - at < 8)
            return 0;
        payload = little_endian(records + at);
        at += 8;
        if (payload > length - at)
            return 0;
        if (kind == 16 && payload == 8 && !memcmp(records + at, "\"commit\"", 8))
            return 1;
        at += (size_t)payload;
    }
    return 0;
}

static int commit_frame(const unsigned char *frame, size_t available, uint64_t tail_sequence,
                        const unsigned char *tail_anchor) {
    if (available < 104)
        return 0;
    uint64_t length = little_endian(frame);
    if (length < 96 || length > available - 8)
        return 0;
    size_t total = (size_t)length + 8;
    /* frame = length, version, sequence, previous, count, records, digest, marker */
    if (little_endian(frame + 16) != tail_sequence ||
        memcmp(frame + total - 40, tail_anchor, 32) ||
        little_endian(frame + total - 8) != UINT64_C(0x35444e45544d4355))
        return 0;
    uint64_t count = little_endian(frame + 56);
    size_t records = 64, end = total - 40;
    if (count == 0 || count > 1024 || records > end)
        return 0;
    return commit_record(frame + records, end - records);
}

static int committed_tail(int directory) {
    unsigned char *tail = NULL;
    size_t tail_length = 0;
    if (!read_file(directory, "journal.tail", &tail, &tail_length) || tail_length != 80) {
        free(tail);
        return 0;
    }
    uint64_t sequence = little_endian(tail + 8);
    DIR *entries = fdopendir(dup(directory));
    if (!entries) {
        free(tail);
        return 0;
    }
    int found = 0;
    struct dirent *entry;
    while (!found && (entry = readdir(entries))) {
        uint64_t first;
        if (!segment_number(entry->d_name, &first))
            continue;
        (void)first;
        unsigned char *segment = NULL;
        size_t size = 0;
        if (!read_file(directory, entry->d_name, &segment, &size))
            continue;
        for (size_t offset = 0; offset + 104 <= size;) {
            uint64_t length = little_endian(segment + offset);
            if (length < 96 || length > size - offset - 8)
                break;
            size_t total = (size_t)length + 8;
            if (commit_frame(segment + offset, size - offset, sequence, tail + 16)) {
                found = 1;
                break;
            }
            offset += total;
        }
        free(segment);
    }
    closedir(entries);
    free(tail);
    return found;
}

static int journal_path(int descriptor, char path[PATH_MAX]) {
    struct stat info;
    if (fstat(descriptor, &info))
        return 0;
    char descriptor_path[64];
    int length = snprintf(descriptor_path, sizeof(descriptor_path), "/proc/self/fd/%d", descriptor);
    if (length < 0 || (size_t)length >= sizeof(descriptor_path))
        return 0;
    ssize_t size = readlink(descriptor_path, path, PATH_MAX - 1);
    if (size <= 0 || size >= PATH_MAX)
        return 0;
    path[size] = '\0';
    if (S_ISDIR(info.st_mode))
        return 1;
    if (!S_ISREG(info.st_mode))
        return 0;
    char *slash = strrchr(path, '/');
    if (!slash || slash == path)
        return 0;
    *slash = '\0';
    return 1;
}

static int armed(int descriptor, const char *arms, char directory[PATH_MAX],
                 char marker[PATH_MAX]) {
    if (!journal_path(descriptor, directory))
        return 0;
    int journal = open(directory, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (journal < 0)
        return 0;
    const char *ticket = strrchr(directory, '/');
    ticket = ticket ? ticket + 1 : directory;
    int length = snprintf(marker, PATH_MAX, "%s/%s", arms, ticket);
    int result = length >= 0 && length < PATH_MAX && !access(marker, F_OK);
    close(journal);
    return result;
}

int fsync(int descriptor) {
    int result = (int)syscall(SYS_fsync, descriptor);
    int saved = errno;
    const char *arms = getenv("UD_TEST_XA_DECISION_ARMS");
    char directory[PATH_MAX], marker[PATH_MAX];
    if (!result && arms && armed(descriptor, arms, directory, marker)) {
        int journal = open(directory, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (journal >= 0) {
            int decided = committed_tail(journal);
            close(journal);
            if (decided && !unlink(marker))
                _exit(86);
        }
    }
    errno = saved;
    return result;
}
