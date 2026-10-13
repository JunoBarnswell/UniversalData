#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "../internal.h"
#include <errno.h>
#include <time.h>
#ifdef _WIN32
#include <windows.h>
#include <io.h>
static wchar_t *wide(const char *s) {
    int n = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, s, -1, NULL, 0);
    if (!n)
        return NULL;
    wchar_t *w = malloc((size_t)n * sizeof(*w));
    if (w && !MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, s, -1, w, n)) {
        free(w);
        return NULL;
    }
    return w;
}
FILE *ud_fopen(const char *p, const char *mode) {
    wchar_t *w = wide(p), *m = wide(mode);
    FILE *f = w && m ? _wfopen(w, m) : NULL;
    free(w);
    free(m);
    return f;
}
#else
#include <sys/stat.h>
#include <sys/file.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#ifdef __linux__
#include <sys/syscall.h>
#endif
FILE *ud_fopen(const char *p, const char *mode) {
    return fopen(p, mode);
}
#endif
ud_status ud_join_path(ud_context *c, const char *a, const char *b, char **out) {
    size_t n = strlen(a), m = strlen(b);
    if (n > SIZE_MAX - m - 2)
        return UD_OVERFLOW;
    char *p = ud_alloc(c, n + m + 2);
    if (!p)
        return UD_RESOURCE;
    memcpy(p, a, n);
    p[n] = '/';
    memcpy(p + n + 1, b, m + 1);
    *out = p;
    return UD_OK;
}
ud_status ud_mkdir(ud_context *c, const char *p) {
#ifdef _WIN32
    wchar_t *w = wide(p);
    if (!w)
        return UD_INVALID;
    BOOL ok = CreateDirectoryW(w, NULL);
    DWORD e = GetLastError();
    DWORD attr = GetFileAttributesW(w);
    free(w);
    if (ok || (e == ERROR_ALREADY_EXISTS && attr != INVALID_FILE_ATTRIBUTES &&
               (attr & FILE_ATTRIBUTE_DIRECTORY)))
        return UD_OK;
#else
    if (!mkdir(p, 0700))
        return UD_OK;
    struct stat st;
    if (errno == EEXIST && !stat(p, &st) && S_ISDIR(st.st_mode))
        return UD_OK;
#endif
    return ud_fail(c, UD_IO, "cannot create directory: %s", p);
}
ud_status ud_file_identity(ud_context *c, const char *p, uint64_t *dev, uint64_t *ino) {
#ifdef _WIN32
    wchar_t *w = wide(p);
    if (!w)
        return UD_INVALID;
    HANDLE h = CreateFileW(w, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING,
                           FILE_ATTRIBUTE_NORMAL, NULL);
    free(w);
    BY_HANDLE_FILE_INFORMATION info = {0};
    BOOL ok = h != INVALID_HANDLE_VALUE && GetFileInformationByHandle(h, &info);
    if (h != INVALID_HANDLE_VALUE)
        CloseHandle(h);
    if (ok && !(info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
        *dev = info.dwVolumeSerialNumber;
        *ino = ((uint64_t)info.nFileIndexHigh << 32) | info.nFileIndexLow;
        return UD_OK;
    }
#else
    struct stat st;
    if (!lstat(p, &st) && S_ISREG(st.st_mode)) {
        *dev = (uint64_t)st.st_dev;
        *ino = (uint64_t)st.st_ino;
        return UD_OK;
    }
#endif
    return ud_fail(c, UD_IO, "missing or non-regular input: %s", p);
}
static ud_status add_name(ud_context *c, char ***list, uint32_t *n, const char *s,
                          const char *suffix) {
    size_t a = strlen(s), b = strlen(suffix);
    if (a < b || strcmp(s + a - b, suffix))
        return UD_OK;
    if (!ud_utf8_valid(s, a) || strchr(s, '/') || strchr(s, '\\'))
        return ud_fail(c, UD_INVALID, "invalid input filename");
    if (*n == UINT32_MAX || (size_t)(*n + 1) > SIZE_MAX / sizeof(char *))
        return UD_RESOURCE;
    char **v = ud_alloc(c, (size_t)(*n + 1) * sizeof(*v));
    if (!v)
        return UD_RESOURCE;
    if (*n)
        memcpy(v, *list, (size_t)*n * sizeof(*v));
    v[*n] = ud_strdup_n(c, s, a);
    if (!v[*n]) {
        ud_free(v);
        return UD_RESOURCE;
    }
    ud_free(*list);
    *list = v;
    (*n)++;
    return UD_OK;
}
static int name_compare(const void *a, const void *b) {
    return strcmp(*(char *const *)a, *(char *const *)b);
}
ud_status ud_list(ud_context *c, const char *dir, const char *suffix, char ***out, uint32_t *n) {
    *out = NULL;
    *n = 0;
    ud_status s = UD_OK;
#ifdef _WIN32
    char *pattern = NULL;
    UD_TRY(ud_join_path(c, dir, "*", &pattern));
    wchar_t *w = wide(pattern);
    ud_free(pattern);
    if (!w)
        return UD_INVALID;
    WIN32_FIND_DATAW d;
    HANDLE h = FindFirstFileW(w, &d);
    free(w);
    if (h == INVALID_HANDLE_VALUE)
        return ud_fail(c, UD_IO, "cannot enumerate %s", dir);
    do {
        if (d.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT))
            continue;
        int len = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, d.cFileName, -1, NULL, 0, NULL,
                                      NULL);
        char *name = len ? ud_alloc(c, (size_t)len) : NULL;
        if (!name) {
            s = UD_RESOURCE;
            break;
        }
        WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, d.cFileName, -1, name, len, NULL, NULL);
        s = add_name(c, out, n, name, suffix);
        ud_free(name);
        if (s)
            break;
    } while (FindNextFileW(h, &d));
    if (!s && GetLastError() != ERROR_NO_MORE_FILES)
        s = UD_IO;
    FindClose(h);
#else
    DIR *d = opendir(dir);
    if (!d)
        return ud_fail(c, UD_IO, "cannot enumerate %s", dir);
    struct dirent *e;
    errno = 0;
    while ((e = readdir(d))) {
        s = add_name(c, out, n, e->d_name, suffix);
        if (s)
            break;
        errno = 0;
    }
    if (!s && errno)
        s = UD_IO;
    closedir(d);
#endif
    if (s) {
        ud_list_free(*out, *n);
        *out = NULL;
        *n = 0;
        return ud_fail(c, s, "directory enumeration failed: %s", dir);
    }
    if (*n > 1)
        qsort(*out, *n, sizeof(char *), name_compare);
    return UD_OK;
}
void ud_list_free(char **v, uint32_t n) {
    for (uint32_t i = 0; i < n; i++)
        ud_free(v[i]);
    ud_free(v);
}
ud_status ud_flush(ud_context *c, FILE *f) {
    if (fflush(f))
        return ud_fail(c, UD_IO, "flush failed");
#ifdef _WIN32
    if (_commit(_fileno(f)))
        return ud_fail(c, UD_IO, "file sync failed");
#else
    if (fsync(fileno(f)))
        return ud_fail(c, UD_IO, "file sync failed");
#endif
    return UD_OK;
}
ud_status ud_sync_parent(ud_context *c, const char *path) {
#ifdef _WIN32
    /* Publication uses MoveFileExW(MOVEFILE_WRITE_THROUGH). */
    (void)c;
    (void)path;
    return UD_OK;
#else
    const char *slash = strrchr(path, '/');
    char *parent = slash ? ud_strdup_n(c, path, slash == path ? 1 : (size_t)(slash - path))
                         : ud_strdup_n(c, ".", 1);
    if (!parent)
        return UD_RESOURCE;
    int flags = O_RDONLY;
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
    int fd = open(parent, flags);
    ud_free(parent);
    if (fd < 0)
        return ud_fail(c, UD_IO, "cannot open publication directory");
    struct stat info;
    int failed = fstat(fd, &info) || !S_ISDIR(info.st_mode) || fsync(fd);
    if (close(fd))
        failed = 1;
    return failed ? ud_fail(c, UD_IO, "publication directory sync failed") : UD_OK;
#endif
}
ud_status ud_replace_file(ud_context *c, const char *from, const char *to) {
#ifdef _WIN32
    wchar_t *source = wide(from), *target = wide(to);
    BOOL ok = source && target &&
              MoveFileExW(source, target, MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH);
    free(source);
    free(target);
    return ok ? UD_OK : ud_fail(c, UD_IO, "atomic replacement failed: %s", to);
#else
    if (rename(from, to))
        return ud_fail(c, UD_IO, "atomic replacement failed: %s", to);
    return ud_sync_parent(c, to);
#endif
}
ud_status ud_publish(ud_context *c, const char *from, const char *to) {
#ifdef _WIN32
    wchar_t *a = wide(from), *b = wide(to);
    BOOL ok = a && b && MoveFileExW(a, b, MOVEFILE_WRITE_THROUGH);
    free(a);
    free(b);
    if (ok)
        return UD_OK;
#else
    /* link() atomically creates a name without replacing an existing generation. */
    if (!link(from, to)) {
        UD_TRY(ud_sync_parent(c, to));
        if (unlink(from))
            return ud_fail(c, UD_IO, "published but temporary unlink failed: %s", from);
        return ud_sync_parent(c, from);
    }
#endif
    return ud_fail(c, UD_IO, "atomic publish failed or destination exists: %s", to);
}
ud_status ud_lock(ud_context *c, const char *path, void **out) {
    *out = NULL;
#ifdef _WIN32
    wchar_t *w = wide(path);
    if (!w)
        return UD_INVALID;
    HANDLE h = CreateFileW(w, GENERIC_READ | GENERIC_WRITE, 0, NULL, OPEN_ALWAYS,
                           FILE_ATTRIBUTE_NORMAL, NULL);
    free(w);
    if (h == INVALID_HANDLE_VALUE)
        return ud_fail(c, UD_BUSY, "state directory is locked: %s", path);
    *out = h;
#else
    int fd = open(path, O_RDWR | O_CREAT, 0600);
    if (fd < 0)
        return ud_fail(c, UD_IO, "cannot open lock: %s", path);
    if (flock(fd, LOCK_EX | LOCK_NB)) {
        close(fd);
        return ud_fail(c, UD_BUSY, "state directory is locked: %s", path);
    }
    *out = (void *)(intptr_t)(fd + 1);
#endif
    return UD_OK;
}
void ud_unlock(void *h) {
    if (!h)
        return;
#ifdef _WIN32
    CloseHandle(h);
#else
    int fd = (int)(intptr_t)h - 1;
    flock(fd, LOCK_UN);
    close(fd);
#endif
}
ud_status ud_remove_file(ud_context *c, const char *path) {
#ifdef _WIN32
    wchar_t *w = wide(path);
    BOOL ok = w && DeleteFileW(w);
    free(w);
    if (ok || GetLastError() == ERROR_FILE_NOT_FOUND)
        return UD_OK;
#else
    if (!unlink(path) || errno == ENOENT)
        return UD_OK;
#endif
    return ud_fail(c, UD_IO, "cannot remove owned temporary file: %s", path);
}
ud_status ud_reserve_file(ud_context *ctx, const char *path) {
    FILE *file = ud_fopen(path, "wbx");
    if (!file)
        return ud_fail(ctx, errno == EEXIST ? UD_BUSY : UD_IO,
                       "cannot reserve an owned temporary file");
    if (fclose(file)) {
        ud_remove_file(ctx, path);
        return UD_IO;
    }
    return UD_OK;
}
uint64_t ud_now_ms(void) {
#ifdef _WIN32
    return GetTickCount64();
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
#endif
}
uint64_t ud_monotonic_ns(void) {
#ifdef _WIN32
    LARGE_INTEGER counter, frequency;
    if (!QueryPerformanceCounter(&counter) || !QueryPerformanceFrequency(&frequency) ||
        frequency.QuadPart <= 0)
        return 0;
    uint64_t ticks = (uint64_t)counter.QuadPart, rate = (uint64_t)frequency.QuadPart;
    return ticks / rate * 1000000000 + ticks % rate * 1000000000 / rate;
#else
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts))
        return 0;
    return (uint64_t)ts.tv_sec * 1000000000 + (uint64_t)ts.tv_nsec;
#endif
}
uint64_t ud_thread_cpu_ns(void) {
#ifdef _WIN32
    FILETIME created, ended, kernel, user;
    if (!GetThreadTimes(GetCurrentThread(), &created, &ended, &kernel, &user))
        return 0;
    ULARGE_INTEGER k, u;
    k.LowPart = kernel.dwLowDateTime;
    k.HighPart = kernel.dwHighDateTime;
    u.LowPart = user.dwLowDateTime;
    u.HighPart = user.dwHighDateTime;
    return (k.QuadPart + u.QuadPart) * 100;
#else
    struct timespec ts;
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts))
        return 0;
    return (uint64_t)ts.tv_sec * 1000000000 + (uint64_t)ts.tv_nsec;
#endif
}
ud_status ud_distinct_directories(ud_context *c, const char *first, const char *second) {
#ifdef _WIN32
    wchar_t *a = wide(first), *b = wide(second);
    HANDLE ha = a ? CreateFileW(a, 0, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
                                OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, NULL)
                  : INVALID_HANDLE_VALUE;
    HANDLE hb = b ? CreateFileW(b, 0, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
                                OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, NULL)
                  : INVALID_HANDLE_VALUE;
    free(a);
    free(b);
    BY_HANDLE_FILE_INFORMATION ia = {0}, ib = {0};
    BOOL ok = ha != INVALID_HANDLE_VALUE && hb != INVALID_HANDLE_VALUE &&
              GetFileInformationByHandle(ha, &ia) && GetFileInformationByHandle(hb, &ib);
    if (ha != INVALID_HANDLE_VALUE)
        CloseHandle(ha);
    if (hb != INVALID_HANDLE_VALUE)
        CloseHandle(hb);
    if (!ok || !(ia.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) ||
        !(ib.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY))
        return ud_fail(c, UD_IO, "input and state directories must exist");
    int same = ia.dwVolumeSerialNumber == ib.dwVolumeSerialNumber &&
               ia.nFileIndexHigh == ib.nFileIndexHigh && ia.nFileIndexLow == ib.nFileIndexLow;
#else
    struct stat a, b;
    if (stat(first, &a) || stat(second, &b) || !S_ISDIR(a.st_mode) || !S_ISDIR(b.st_mode))
        return ud_fail(c, UD_IO, "input and state directories must exist");
    int same = a.st_dev == b.st_dev && a.st_ino == b.st_ino;
#endif
    return same ? ud_fail(c, UD_INVALID, "input and state resolve to the same directory") : UD_OK;
}
void ud_sleep_ms(uint32_t ms) {
#ifdef _WIN32
    Sleep(ms);
#else
    struct timespec ts = {(time_t)(ms / 1000), (long)(ms % 1000) * 1000000};
    while (nanosleep(&ts, &ts) && errno == EINTR) {
    }
#endif
}
int ud_seek(FILE *f, uint64_t offset) {
    if (offset > INT64_MAX)
        return -1;
#ifdef _WIN32
    return _fseeki64(f, (int64_t)offset, SEEK_SET);
#else
    return fseeko(f, (off_t)offset, SEEK_SET);
#endif
}
int64_t ud_tell(FILE *f) {
#ifdef _WIN32
    return _ftelli64(f);
#else
    return (int64_t)ftello(f);
#endif
}

ud_status ud_path_kind(ud_context *c, const char *path, int *kind) {
    *kind = 0;
#ifdef _WIN32
    wchar_t *p = wide(path);
    if (!p)
        return UD_INVALID;
    DWORD attr = GetFileAttributesW(p), error = GetLastError();
    free(p);
    if (attr == INVALID_FILE_ATTRIBUTES)
        return error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND
                   ? UD_OK
                   : ud_fail(c, UD_IO, "cannot inspect file resource");
    *kind = attr & FILE_ATTRIBUTE_REPARSE_POINT ? 3 : attr & FILE_ATTRIBUTE_DIRECTORY ? 2 : 1;
#else
    struct stat info;
    if (lstat(path, &info))
        return errno == ENOENT ? UD_OK : ud_fail(c, UD_IO, "cannot inspect file resource");
    *kind = S_ISREG(info.st_mode) ? 1 : S_ISDIR(info.st_mode) ? 2 : 3;
#endif
    return UD_OK;
}
ud_status ud_path_no_symlinks(ud_context *c, const char *path) {
    size_t n = strlen(path);
    char *part = ud_strdup_n(c, path, n);
    if (!part)
        return UD_RESOURCE;
    ud_status s = UD_OK;
#ifdef _WIN32
    size_t start = 3;
#else
    size_t start = 1;
#endif
    for (size_t i = start; !s && i <= n; i++) {
        if (i < n && path[i] != '/' && path[i] != '\\')
            continue;
#ifndef _WIN32
        if (i < n && path[i] == '\\')
            continue;
#endif
        if (i == start || (i < n && path[i - 1] == '/'))
            continue;
        char old = part[i];
        part[i] = 0;
        int kind = 0;
        s = ud_path_kind(c, part, &kind);
        part[i] = old;
        if (!s && kind == 3)
            s = UD_SCHEMA;
        if (!s && i < n && kind != 2)
            s = UD_IO;
    }
    ud_free(part);
    return s;
}
ud_status ud_open_regular(ud_context *c, const char *path, FILE **out) {
    *out = NULL;
#ifdef _WIN32
    int kind;
    UD_TRY(ud_path_kind(c, path, &kind));
    if (kind != 1)
        return UD_IO;
    FILE *f = ud_fopen(path, "rb");
    if (!f)
        return UD_IO;
    *out = f;
#else
    int fd = open(path, O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0)
        return ud_fail(c, UD_IO, "cannot open regular file");
    struct stat info;
    if (fstat(fd, &info) || !S_ISREG(info.st_mode)) {
        close(fd);
        return UD_SCHEMA;
    }
    FILE *f = fdopen(fd, "rb");
    if (!f) {
        close(fd);
        return UD_IO;
    }
    *out = f;
#endif
    return UD_OK;
}
ud_status ud_move_noreplace(ud_context *c, const char *from, const char *to) {
#ifdef _WIN32
    wchar_t *a = wide(from), *b = wide(to);
    BOOL ok = a && b && MoveFileExW(a, b, MOVEFILE_WRITE_THROUGH);
    DWORD error = GetLastError();
    free(a);
    free(b);
    if (!ok)
        return error == ERROR_ALREADY_EXISTS || error == ERROR_FILE_EXISTS ? UD_BUSY : UD_IO;
#elif defined(__linux__) && defined(SYS_renameat2)
    if (syscall(SYS_renameat2, AT_FDCWD, from, AT_FDCWD, to, 1))
        return errno == EEXIST                      ? UD_BUSY
               : errno == ENOSYS || errno == EINVAL ? UD_UNSUPPORTED
                                                    : UD_IO;
#else
    (void)c;
    (void)from;
    (void)to;
    return UD_UNSUPPORTED;
#endif
    ud_status s = ud_sync_parent(c, to);
    if (!s)
        s = ud_sync_parent(c, from);
    return s ? UD_COMMIT_UNKNOWN : UD_OK;
}
