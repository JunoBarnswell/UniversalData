using System.Net.Http.Headers;
using System.Reflection;
using System.Text.Json;
using UniversalData;
using UniversalData.Hosting;

namespace UniversalData.CommandLine;

internal static class JobCommand
{
    internal static async Task<int> RunAsync(string[] args, CancellationToken cancellation = default)
    {
        JobCommandOptions options;
        try { options = JobCommandOptions.Parse(args); }
        catch (ArgumentException error)
        {
            Console.Error.WriteLine(error.Message);
            return 2;
        }
        if (options.Help)
        {
            Console.WriteLine("usage: ud_cli run|publish|export|validate|preview|status|stop|cancel|resume JOB_ID [options]");
            return 0;
        }

        try
        {
            var configuration = DataConfiguration.Load(options.ConfigPath, options.ParametersFile, options.Overrides);
            return options.Command switch
            {
                "export" => await ExportAsync(configuration, options, cancellation).ConfigureAwait(false),
                "publish" => await PublishAsync(configuration, options, cancellation).ConfigureAwait(false),
                "validate" => await ValidateAsync(configuration, options, cancellation).ConfigureAwait(false),
                "preview" => await PreviewAsync(configuration, options, cancellation).ConfigureAwait(false),
                "status" => await ExecutionCommandAsync(configuration, options, "status", cancellation).ConfigureAwait(false),
                "stop" => await ExecutionCommandAsync(configuration, options, "stop", cancellation).ConfigureAwait(false),
                "cancel" => await ExecutionCommandAsync(configuration, options, "cancel", cancellation).ConfigureAwait(false),
                "resume" => await ExecutionCommandAsync(configuration, options, "resume", cancellation).ConfigureAwait(false),
                _ => await RunLocalAsync(configuration, options, cancellation).ConfigureAwait(false)
            };
        }
        catch (OperationCanceledException)
        {
            return Report(new(StatusCode.Cancelled,
                new(ErrorSource.Transport, StatusCode.Cancelled, "Job command cancelled")));
        }
        catch (Exception error) when (error is ArgumentException or IOException or InvalidDataException or KeyNotFoundException or
            MissingMemberException or TypeLoadException or UniversalDataException)
        {
            StatusCode code = error is UniversalDataException native ? native.Code : StatusCode.Invalid;
            return Report(new(code, (error as UniversalDataException)?.Detail ??
                new(ErrorSource.Native, code, error.Message)));
        }
    }

    private static async Task<int> RunLocalAsync(DataConfiguration configuration,
        JobCommandOptions options, CancellationToken cancellation)
    {
        await using var client = configuration.CreateClient();
        Package package = ResolvePackage(configuration, client, options.JobId, publish: false);
        DataJobResult result = await client.RunAsync(package, configuration.Parameters(options.JobId), cancellation).ConfigureAwait(false);
        Console.WriteLine(JsonSerializer.Serialize(result, HostPolicy.JsonOptions));
        return 0;
    }

    private static async Task<int> ExportAsync(DataConfiguration configuration,
        JobCommandOptions options, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        await using var client = configuration.CreateClient();
        Package package = ResolvePackage(configuration, client, options.JobId, publish: true);

        string document = package.ToJson();
        if (options.OutputPath is null)
        {
            Console.WriteLine(document);
            return 0;
        }
        string output = Path.GetFullPath(options.OutputPath);
        Directory.CreateDirectory(Path.GetDirectoryName(output)!);
        await File.WriteAllTextAsync(output, document, cancellation).ConfigureAwait(false);
        Console.WriteLine(JsonSerializer.Serialize(new { path = output, id = options.JobId }, HostPolicy.JsonOptions));
        return 0;
    }

    private static async Task<int> PublishAsync(DataConfiguration configuration,
        JobCommandOptions options, CancellationToken cancellation)
    {
        await using var client = configuration.CreateClient();
        Package package = ResolvePackage(configuration, client, options.JobId, publish: true);

        string baseUrl = options.ManagementBaseUrl ?? configuration.ManagementBaseUrl ??
            Environment.GetEnvironmentVariable("UD_MANAGEMENT_URL") ??
            throw new ArgumentException("publish requires --management-url or management.baseUrl");
        if (!Uri.TryCreate(baseUrl, UriKind.Absolute, out var baseUri) || baseUri.Scheme is not ("http" or "https"))
            throw new ArgumentException("Management URL must be an absolute HTTP or HTTPS URI");

        using var http = new HttpClient { BaseAddress = baseUri };
        ConfigureManagement(http, configuration, options);
        string requestId = options.RequestId ?? Environment.GetEnvironmentVariable("UD_REQUEST_ID") ?? Guid.NewGuid().ToString("N");
        string expectedRevision = options.ExpectedPackageRevision ?? configuration.PackageRevision(options.JobId);
        JobSchedule[] schedules = configuration.Schedules(options.JobId);
        bool hasSchedules = schedules.Length != 0;
        string? parameters = options.Run || hasSchedules ? configuration.Parameters(options.JobId) : null;
        string? csrf = options.Csrf ?? configuration.ManagementCsrf ?? Environment.GetEnvironmentVariable("UD_MANAGEMENT_CSRF");
        JobEnvironmentReference? environment = options.EnvironmentId is null ? configuration.ManagementEnvironment :
            new JobEnvironmentReference(options.EnvironmentId, options.EnvironmentRevision!);
        var result = await client.PublishAsync(package, new EtlManagementClient(http, csrf),
            new JobPublicationOptions(requestId, expectedRevision, schedules.Length == 0 ? null : schedules,
                options.Run, parameters, environment), cancellation)
            .ConfigureAwait(false);
        if (!result.IsSuccess)
            return Report(new(result.Code, result.Error));
        Console.WriteLine(JsonSerializer.Serialize(result.Value, HostPolicy.JsonOptions));
        return 0;
    }

    private static async Task<int> ValidateAsync(DataConfiguration configuration,
        JobCommandOptions options, CancellationToken cancellation)
    {
        await using var client = configuration.CreateClient();
        Package package = ResolvePackage(configuration, client, options.JobId, publish: false);
        var result = await client.ValidateAsync(package, configuration.Parameters(options.JobId), cancellation)
            .ConfigureAwait(false);
        Console.WriteLine(result.GetRawText());
        return result.IsValid ? 0 : 1;
    }

    private static async Task<int> PreviewAsync(DataConfiguration configuration,
        JobCommandOptions options, CancellationToken cancellation)
    {
        await using var client = configuration.CreateClient();
        Package package = ResolvePackage(configuration, client, options.JobId, publish: false);
        var result = await client.PreviewAsync(package, configuration.Parameters(options.JobId), options.SourceRows,
            options.SampleRows, cancellation).ConfigureAwait(false);
        Console.WriteLine(result.GetRawText());
        return result.GetProperty("result").GetInt32() == 0 ? 0 : 1;
    }

    private static async Task<int> ExecutionCommandAsync(DataConfiguration configuration,
        JobCommandOptions options, string command, CancellationToken cancellation)
    {
        string baseUrl = options.ManagementBaseUrl ?? configuration.ManagementBaseUrl ??
            Environment.GetEnvironmentVariable("UD_MANAGEMENT_URL") ??
            throw new ArgumentException($"{command} requires --management-url or management.baseUrl");
        if (!Uri.TryCreate(baseUrl, UriKind.Absolute, out var baseUri) || baseUri.Scheme is not ("http" or "https"))
            throw new ArgumentException("Management URL must be an absolute HTTP or HTTPS URI");
        using var http = new HttpClient { BaseAddress = baseUri };
        ConfigureManagement(http, configuration, options);
        string? csrf = options.Csrf ?? configuration.ManagementCsrf ?? Environment.GetEnvironmentVariable("UD_MANAGEMENT_CSRF");
        var management = new EtlManagementClient(http, csrf);
        string id = options.ExecutionId ?? options.JobId;
        Result result;
        object? value = null;
        switch (command)
        {
            case "status":
                var status = await management.ExecutionAsync(id, cancellation).ConfigureAwait(false);
                result = new(status.Code, status.Error); value = status.Value; break;
            case "stop": result = await management.StopExecutionAsync(id, cancellation).ConfigureAwait(false); break;
            case "cancel": result = await management.CancelExecutionAsync(id, cancellation).ConfigureAwait(false); break;
            case "resume":
                var resumed = await management.ResumeExecutionAsync(id, cancellation).ConfigureAwait(false);
                result = new(resumed.Code, resumed.Error); value = resumed.Value; break;
            default: throw new ArgumentOutOfRangeException(nameof(command));
        }
        if (!result.IsSuccess) return Report(result);
        Console.WriteLine(JsonSerializer.Serialize(value is null ? result : value, HostPolicy.JsonOptions));
        return 0;
    }

    private static void ConfigureManagement(HttpClient http, DataConfiguration configuration, JobCommandOptions options)
    {
        string? token = options.BearerToken ?? configuration.ManagementBearerToken ?? Environment.GetEnvironmentVariable("UD_MANAGEMENT_TOKEN");
        if (!string.IsNullOrWhiteSpace(token)) http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        string? cookie = options.Cookie ?? configuration.ManagementCookie ?? Environment.GetEnvironmentVariable("UD_MANAGEMENT_COOKIE");
        if (!string.IsNullOrWhiteSpace(cookie)) http.DefaultRequestHeaders.TryAddWithoutValidation("Cookie", cookie);
        foreach (var pair in configuration.ManagementHeaders)
            http.DefaultRequestHeaders.TryAddWithoutValidation(pair.Key, pair.Value);
    }

    private static Package ResolvePackage(DataConfiguration configuration, DataClient client, string jobId, bool publish)
    {
        List<DataJob> declarations = FindDeclarations(configuration, jobId);
        if (configuration.HasJob(jobId))
        {
            if (declarations.Count != 0)
                throw new InvalidDataException($"Job '{jobId}' is declared in both YAML and a configured C# assembly");
            return configuration.Package(jobId);
        }
        if (declarations.Count == 0)
            throw new KeyNotFoundException($"Job '{jobId}' is not declared in YAML or a configured C# assembly");
        if (declarations.Count > 1)
            throw new InvalidDataException($"Job '{jobId}' is declared more than once in configured C# assemblies");
        return client.Compile(declarations[0], publish);
    }

    private static List<DataJob> FindDeclarations(DataConfiguration configuration, string jobId)
    {
        var result = new List<DataJob>();
        foreach (string path in configuration.AssemblyPaths)
        {
            Assembly assembly = Assembly.LoadFrom(path);
            foreach (Type type in assembly.GetExportedTypes())
            {
                foreach (PropertyInfo property in type.GetProperties(BindingFlags.Public | BindingFlags.Static))
                {
                    if (property.GetMethod is null || !typeof(DataJob).IsAssignableFrom(property.PropertyType)) continue;
                    AddDeclaration(property.GetValue(null) as DataJob, jobId, path, result);
                }
                foreach (FieldInfo field in type.GetFields(BindingFlags.Public | BindingFlags.Static))
                {
                    if (!typeof(DataJob).IsAssignableFrom(field.FieldType)) continue;
                    AddDeclaration(field.GetValue(null) as DataJob, jobId, path, result);
                }
            }
        }
        return result;
    }

    private static void AddDeclaration(DataJob? value, string jobId, string path, List<DataJob> result)
    {
        if (value is null) throw new InvalidDataException($"C# DataJob declaration in '{path}' is null");
        if (value.Id.Equals(jobId, StringComparison.Ordinal)) result.Add(value);
    }

    private static int Report(Result result)
    {
        Console.Error.WriteLine(JsonSerializer.Serialize(result, HostPolicy.JsonOptions));
        return (int)result.Code;
    }
}
