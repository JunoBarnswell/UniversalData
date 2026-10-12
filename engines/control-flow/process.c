#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "../package/internal.h"
#include "ud/connection_manager.h"
#include <errno.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/syscall.h>
#endif
extern char **environ;
#endif
typedef struct process_config {
    ud_string connection, cwd, exit_variable;
    const ud_json *arguments;
    uint64_t timeout, expected;
} process_config;
static ud_status read_config(const ud_component_context *c, ud_string text, ud_json **out,
                             process_config *p) {
    memset(p, 0, sizeof(*p));
    p->timeout = 60000;
    *out = NULL;
    UD_TRY(ud_json_parse(c->memory, text.data, text.length, out, NULL));
    static const char *keys[] = {"connection", "arguments",        "workingDirectory",
                                 "timeoutMs",  "exitCodeVariable", "expectedExitCode"};
    ud_status s = ud_json_keys(*out, keys, 6);
    if (!s)
        s = ud_json_string(ud_member(*out, "connection"), &p->connection);
    p->arguments = ud_member(*out, "arguments");
    if (!s && p->arguments &&
        (ud_json_kind(p->arguments) != UD_JSON_ARRAY || ud_json_size(p->arguments) > 1024))
        s = UD_SCHEMA;
    uint64_t total_arguments = 0;
    for (uint32_t i = 0; !s && i < ud_json_size(p->arguments); i++) {
        const ud_json *arg = ud_json_at(p->arguments, i);
        const char *value = NULL;
        uint32_t n = 0;
        if (ud_json_kind(arg) != UD_JSON_STRING)
            s = UD_SCHEMA;
        if (!s)
            s = ud_json_text(arg, &value, &n);
        if (!s && (n > 32767 || memchr(value, 0, n)))
            s = UD_SCHEMA;
        if (!s && (total_arguments += n) > 1048576)
            s = UD_RESOURCE;
    }
    if (!s && ud_member(*out, "timeoutMs"))
        s = ud_json_uint(ud_member(*out, "timeoutMs"), &p->timeout);
    if (!s && (!p->timeout || p->timeout > 86400000))
        s = UD_SCHEMA;
    if (!s && ud_member(*out, "expectedExitCode"))
        s = ud_json_uint(ud_member(*out, "expectedExitCode"), &p->expected);
    if (!s && p->expected > UINT32_MAX)
        s = UD_SCHEMA;
    if (!s && ud_member(*out, "workingDirectory"))
        s = ud_json_string(ud_member(*out, "workingDirectory"), &p->cwd);
    if (!s && ud_member(*out, "exitCodeVariable")) {
        s = ud_json_string(ud_member(*out, "exitCodeVariable"), &p->exit_variable);
        ud_variable *v = NULL;
        if (!s)
            s = ud_variable_writable(c->activation, p->exit_variable, &v);
        if (!s && v->value.type != UD_INT64)
            s = UD_SCHEMA;
    }
    ud_string ids[] = {p->connection, p->cwd};
    for (int i = 0; !s && i < 2; i++)
        if (ids[i].length) {
            const ud_json *def = NULL;
            int owner;
            s = ud_connection_definition(c->activation, ids[i], &def, &owner);
            const ud_json *driver = ud_member(def, "driver");
            ud_string value = {0};
            if (!s && ud_json_kind(driver) == UD_JSON_STRING) {
                s = ud_json_string(driver, &value);
                if (!s && !ud_named(value, i ? "file" : "process"))
                    s = UD_UNSUPPORTED;
            }
        }
    return s;
}
static ud_status configure(void *user, const ud_component_context *c, ud_string text,
                           const ud_batch *const *in, ud_batch **out) {
    (void)user;
    (void)in;
    (void)out;
    ud_json *doc = NULL;
    process_config p;
    ud_status s = read_config(c, text, &doc, &p);
    ud_json_destroy(doc);
    return s;
}
static ud_status interrupted(ud_activation *e, uint64_t deadline) {
    ud_status s = ud_activation_check(e);
    if (!s && ud_now_ms() >= deadline)
        s = UD_TIMEOUT;
    return s;
}
#ifndef _WIN32
static void child_fail(int fd) {
    int error = errno;
    (void)write(fd, &error, sizeof(error));
    _exit(127);
}
static ud_status launch(ud_activation *e, char **argv, const char *cwd, uint64_t deadline,
                        int64_t *code) {
    long maximum = sysconf(_SC_OPEN_MAX);
    if (maximum < 0)
        maximum = 65536;
    int errors[2];
    if (pipe2(errors, O_CLOEXEC | O_NONBLOCK))
        return UD_IO;
    pid_t pid = fork();
    if (pid < 0) {
        close(errors[0]);
        close(errors[1]);
        return UD_IO;
    }
    if (!pid) {
        int error_fd = fcntl(errors[1], F_DUPFD_CLOEXEC, 3);
        if (error_fd < 0)
            child_fail(errors[1]);
        close(errors[0]);
        if (setsid() < 0)
            child_fail(error_fd);
        int null = open("/dev/null", O_RDWR);
        if (null < 0 || dup2(null, 0) < 0 || dup2(null, 1) < 0 || dup2(null, 2) < 0)
            child_fail(error_fd);
        if (error_fd != 3 && dup3(error_fd, 3, O_CLOEXEC) < 0)
            child_fail(error_fd);
#ifdef __linux__
#ifdef SYS_close_range
        if (syscall(SYS_close_range, 4, UINT_MAX, 0))
#endif
#endif
        {
            for (int fd = 4; fd < maximum; fd++)
                close(fd);
        }
        if (cwd && chdir(cwd))
            child_fail(3);
        execve(argv[0], argv, environ);
        child_fail(3);
    }
    close(errors[1]);
    ud_status s = UD_OK;
    siginfo_t info;
    int status = 0, lost = 0;
    for (;;) {
        memset(&info, 0, sizeof(info));
        if (waitid(P_PID, (id_t)pid, &info, WEXITED | WNOWAIT | WNOHANG) && errno != EINTR) {
            s = UD_IO;
            lost = 1;
            break;
        }
        if (info.si_pid == pid)
            break;
        s = interrupted(e, deadline);
        if (s)
            break;
        ud_sleep_ms(10);
    }
    /* Keep the leader's zombie reserved until group cleanup: never signal a
     * process-group id after reaping and allowing the PID to be reused. */
    if (!lost) {
        (void)kill(-pid, SIGKILL);
        if (s)
            (void)kill(pid, SIGKILL);
        pid_t reaped;
        do {
            reaped = waitpid(pid, &status, 0);
        } while (reaped < 0 && errno == EINTR);
        if (reaped != pid) {
            lost = 1;
            if (!s)
                s = UD_IO;
        }
    }
    int error = 0;
    ssize_t count;
    do {
        count = read(errors[0], &error, sizeof(error));
    } while (count < 0 && errno == EINTR);
    close(errors[0]);
    if (!s && count > 0)
        s = UD_IO;
    *code = lost                  ? -1
            : WIFEXITED(status)   ? WEXITSTATUS(status)
            : WIFSIGNALED(status) ? -WTERMSIG(status)
                                  : -1;
    return s;
}
#else
static wchar_t *wide(ud_context *c, const char *text) {
    int n = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, text, -1, NULL, 0);
    if (!n)
        return NULL;
    wchar_t *out = ud_alloc(c, (size_t)n * sizeof(*out));
    if (out && !MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, text, -1, out, n)) {
        ud_free(out);
        return NULL;
    }
    return out;
}
static ud_status wide_put(ud_bytes *b, wchar_t c) {
    return ud_bytes_put(b, &c, sizeof(c));
}
static ud_status quoted(ud_bytes *b, const wchar_t *arg) {
    UD_TRY(wide_put(b, L'"'));
    size_t slashes = 0;
    for (const wchar_t *p = arg;; p++) {
        if (*p == L'\\') {
            slashes++;
            continue;
        }
        size_t count = (*p == L'"' || !*p) ? slashes * 2 : slashes;
        for (size_t i = 0; i < count; i++)
            UD_TRY(wide_put(b, L'\\'));
        if (*p == L'"')
            UD_TRY(wide_put(b, L'\\'));
        slashes = 0;
        if (!*p)
            break;
        UD_TRY(wide_put(b, *p));
    }
    return wide_put(b, L'"');
}
static ud_status launch(ud_activation *e, char **argv, const char *cwd, uint64_t deadline,
                        int64_t *code) {
    wchar_t *program = wide(e->execution->ctx, argv[0]),
            *directory = cwd ? wide(e->execution->ctx, cwd) : NULL;
    ud_status s = program && (!cwd || directory) ? UD_OK : UD_RESOURCE;
    ud_bytes command = {.ctx = e->execution->ctx};
    for (int i = 0; !s && argv[i]; i++) {
        wchar_t *arg = wide(e->execution->ctx, argv[i]);
        if (!arg) {
            s = UD_RESOURCE;
            break;
        }
        if (i)
            s = wide_put(&command, L' ');
        if (!s)
            s = quoted(&command, arg);
        ud_free(arg);
    }
    if (!s)
        s = wide_put(&command, 0);
    if (!s && command.size / sizeof(wchar_t) > 32767)
        s = UD_OVERFLOW;
    SECURITY_ATTRIBUTES security = {sizeof(security), NULL, TRUE};
    HANDLE null = INVALID_HANDLE_VALUE, job = NULL;
    PROCESS_INFORMATION process = {0};
    STARTUPINFOEXW startup = {0};
    startup.StartupInfo.cb = sizeof(startup);
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    SIZE_T size = 0;
    LPPROC_THREAD_ATTRIBUTE_LIST attributes = NULL;
    if (!s) {
        null = CreateFileW(L"NUL", GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
                           &security, OPEN_EXISTING, 0, NULL);
        if (null == INVALID_HANDLE_VALUE)
            s = UD_IO;
    }
    if (!s) {
        InitializeProcThreadAttributeList(NULL, 1, 0, &size);
        attributes = ud_alloc(e->execution->ctx, size);
        if (!attributes)
            s = UD_RESOURCE;
        else if (!InitializeProcThreadAttributeList(attributes, 1, 0, &size))
            s = UD_IO;
    }
    int initialized = !s;
    if (!s && !UpdateProcThreadAttribute(attributes, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, &null,
                                         sizeof(null), NULL, NULL))
        s = UD_IO;
    startup.lpAttributeList = attributes;
    startup.StartupInfo.hStdInput = startup.StartupInfo.hStdOutput = startup.StartupInfo.hStdError =
        null;
    if (!s) {
        job = CreateJobObjectW(NULL, NULL);
        if (!job)
            s = UD_IO;
    }
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits = {0};
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    if (!s &&
        !SetInformationJobObject(job, JobObjectExtendedLimitInformation, &limits, sizeof(limits)))
        s = UD_IO;
    if (!s && !CreateProcessW(program, (wchar_t *)command.data, NULL, NULL, TRUE,
                              CREATE_SUSPENDED | CREATE_NO_WINDOW | EXTENDED_STARTUPINFO_PRESENT,
                              NULL, directory, &startup.StartupInfo, &process))
        s = UD_IO;
    if (!s && !AssignProcessToJobObject(job, process.hProcess))
        s = UD_IO;
    if (!s && ResumeThread(process.hThread) == (DWORD)-1)
        s = UD_IO;
    while (!s) {
        DWORD state = WaitForSingleObject(process.hProcess, 10);
        if (state == WAIT_OBJECT_0) {
            DWORD exit;
            if (!GetExitCodeProcess(process.hProcess, &exit))
                s = UD_IO;
            else
                *code = exit;
            break;
        }
        if (state == WAIT_FAILED) {
            s = UD_IO;
            break;
        }
        s = interrupted(e, deadline);
    }
    if (s && process.hProcess) {
        TerminateProcess(process.hProcess, 1);
        if (job)
            TerminateJobObject(job, 1);
        WaitForSingleObject(process.hProcess, INFINITE);
    }
    if (job)
        CloseHandle(job);
    if (process.hThread)
        CloseHandle(process.hThread);
    if (process.hProcess)
        CloseHandle(process.hProcess);
    if (initialized)
        DeleteProcThreadAttributeList(attributes);
    ud_free(attributes);
    if (null != INVALID_HANDLE_VALUE)
        CloseHandle(null);
    ud_bytes_clear(&command);
    ud_free(program);
    ud_free(directory);
    return s;
}
#endif
static ud_status invoke(void *user, const ud_component_context *c, ud_string text) {
    (void)user;
    ud_json *doc = NULL;
    process_config p;
    ud_status s = read_config(c, text, &doc, &p);
    ud_connection_lease *program = NULL, *directory = NULL;
    ud_string path = {0}, cwd_path = {0};
    if (!s)
        s = ud_acquire_process_connection(c->activation, p.connection, &program);
    if (!s)
        s = ud_connection_lease_authorize(program, UD_ACCESS_EXECUTE);
    if (!s)
        s = ud_process_connection_path(program, &path);
    if (!s && p.cwd.length)
        s = ud_acquire_file_connection(c->activation, p.cwd, &directory);
    if (!s && directory)
        s = ud_connection_lease_authorize(directory, UD_ACCESS_READ);
    if (!s && directory)
        s = ud_connection_lease_path(directory, &cwd_path);
    uint32_t count = ud_json_size(p.arguments);
    char **argv = NULL, *cwd = NULL;
    if (!s) {
        argv = ud_alloc(c->memory, (count + 2) * sizeof(*argv));
        if (!argv)
            s = UD_RESOURCE;
    }
    if (!s) {
        argv[0] = ud_strdup_n(c->memory, path.data, path.length);
        if (!argv[0])
            s = UD_RESOURCE;
    }
    if (!s && cwd_path.length) {
        cwd = ud_strdup_n(c->memory, cwd_path.data, cwd_path.length);
        if (!cwd)
            s = UD_RESOURCE;
    }
    for (uint32_t i = 0; !s && i < count; i++) {
        const char *arg;
        uint32_t n = 0;
        s = ud_json_text(ud_json_at(p.arguments, i), &arg, &n);
        if (!s) {
            argv[i + 1] = ud_strdup_n(c->memory, arg, n);
            if (!argv[i + 1])
                s = UD_RESOURCE;
        }
    }
    uint64_t now = ud_now_ms(),
             deadline = p.timeout > UINT64_MAX - now ? UINT64_MAX : now + p.timeout;
    int64_t code = -1;
    if (!s)
        s = ud_activation_check(c->activation);
    if (!s)
        s = launch(c->activation, argv, cwd, deadline, &code);
    if (p.exit_variable.length && code != -1 && !ud_is_cancelled(c->memory)) {
        ud_value value = {.type = UD_INT64, .integer = code};
        ud_status saved = ud_activation_set_variable(c->activation, p.exit_variable, &value, NULL);
        if (!s)
            s = saved;
    }
    if (!s && code != (int64_t)p.expected)
        s = UD_IO;
    if (argv) {
        for (uint32_t i = 0; i < count + 1; i++)
            ud_free(argv[i]);
        ud_free(argv);
    }
    ud_free(cwd);
    ud_status closed = ud_connection_lease_release(directory);
    if (!s)
        s = closed;
    closed = ud_connection_lease_release(program);
    if (!s)
        s = closed;
    ud_json_destroy(doc);
    return s;
}
ud_status ud_process_register(ud_registry *registry) {
    static const char schema[] =
        "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"connection\"],"
        "\"properties\":{\"connection\":{\"type\":\"string\"},\"arguments\":{\"type\":\"array\","
        "\"maxItems\":1024,\"items\":{\"type\":\"string\"}},\"workingDirectory\":{\"type\":"
        "\"string\"},\"timeoutMs\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":86400000},"
        "\"expectedExitCode\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":4294967295},"
        "\"exitCodeVariable\":{\"type\":\"string\"}}}";
    ud_component_descriptor d = {.struct_size = sizeof(d),
                                 .abi_version = UD_ABI_VERSION,
                                 .id = {"process.execute", 15},
                                 .name = {"Execute Process", 15},
                                 .version = {"1.0.0", 5},
                                 .configuration_schema = {schema, sizeof(schema) - 1},
                                 .category = UD_COMPONENT_TASK,
                                 .synchronous_input = -1,
                                 .error_output = -1,
                                 .configure = configure,
                                 .invoke = invoke};
    return ud_registry_register(registry, &d);
}