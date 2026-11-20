/* Linux test-only crash injection at the Native transaction decision's durable publication. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

static uint64_t little_endian(const unsigned char *value) {
    uint64_t result = 0;
    for (unsigned index = 0; index < 8; ++index)
        result |= (uint64_t)value[index] << (8 * index);
    return result;
}

static ssize_t read_at(int directory, const char *name, unsigned char *buffer, size_t capacity) {
    int file = openat(directory, name, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (file < 0)
        return -1;
    ssize_t size = read(file, buffer, capacity);
    close(file);
    return size;
}

static int committed_tail(int directory) {
    unsigned char tail[80];
    if (read_at(directory, "journal.tail", tail, sizeof(tail)) != (ssize_t)sizeof(tail))
        return 0;
    unsigned char segment[65536];
    ssize_t size = read_at(directory, "segment-00000000000000000001.udg", segment, sizeof(segment));
    if (size < 104 || size == (ssize_t)sizeof(segment))
        return 0;
    uint64_t sequence = little_endian(tail + 8);
    /* Each frame includes its length and version, sequence, previous digest and record count,
     * then records and their durable frame digest and commit marker. */
    for (size_t offset = 0; (size_t)size - offset >= 104;) {
        const unsigned char *frame = segment + offset;
        uint64_t length = little_endian(frame);
        if (length < 96 || length > (size_t)size - offset - 8)
            return 0;
        size_t total = (size_t)length + 8;
        if (little_endian(frame + 16) == sequence) {
            return total == 208 && little_endian(frame + 8) == little_endian(tail) &&
                   little_endian(frame + 56) == 1 &&
                   little_endian(frame + 64) == 16 && /* RECORD_TRANSACTION_DECISION */
                   little_endian(frame + 72) == 64 && little_endian(frame + 144) == 0 &&
                   little_endian(frame + 152) == 8 && !memcmp(frame + 160, "\"commit\"", 8) &&
                   !memcmp(frame + 168, tail + 16, 32) &&
                   little_endian(frame + 200) == UINT64_C(0x35444e45544d4355);
        }
        offset += total;
    }
    return 0;
}

static int armed(int directory, const char *arms) {
    static const char suffix[] = "/00000001/native";
    static const char prefix[] = "/sdk-runs/";
    struct stat info;
    if (fstat(directory, &info) || !S_ISDIR(info.st_mode))
        return 0;
    char descriptor[64], path[PATH_MAX];
    snprintf(descriptor, sizeof(descriptor), "/proc/self/fd/%d", directory);
    ssize_t size = readlink(descriptor, path, sizeof(path) - 1);
    if (size < (ssize_t)(sizeof(prefix) + sizeof(suffix) + 30))
        return 0;
    path[size] = '\0';
    const char *end = path + size - (sizeof(suffix) - 1);
    const char *run = end - 32;
    if (strcmp(end, suffix) || memcmp(run - (sizeof(prefix) - 1), prefix, sizeof(prefix) - 1))
        return 0;
    char marker[PATH_MAX];
    int length = snprintf(marker, sizeof(marker), "%s/%.32s", arms, run);
    return length >= 0 && (size_t)length < sizeof(marker) && !access(marker, F_OK);
}

int fsync(int descriptor) {
    int result = (int)syscall(SYS_fsync, descriptor);
    int saved = errno;
    const char *arms = getenv("UD_TEST_XA_DECISION_ARMS");
    if (!result && arms && armed(descriptor, arms) && committed_tail(descriptor))
        _exit(86);
    errno = saved;
    return result;
}
