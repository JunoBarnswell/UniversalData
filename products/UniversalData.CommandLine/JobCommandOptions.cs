using System.Text.Json;
using UniversalData;

namespace UniversalData.CommandLine;

internal sealed record JobCommandOptions(
    string Command,
    string JobId,
    string? ConfigPath,
    string? ParametersFile,
    Dictionary<string, JsonElement> Overrides,
    string? OutputPath,
    string? RequestId,
    string? ExpectedPackageRevision,
    string? ManagementBaseUrl,
    string? BearerToken,
    string? Cookie,
    string? Csrf,
    string? EnvironmentId,
    string? EnvironmentRevision,
    string? ExecutionId,
    uint SourceRows,
    uint SampleRows,
    bool Run,
    bool Help)
{
    internal static JobCommandOptions Parse(string[] args)
    {
        if (args.Length < 2 || args[0] is not ("run" or "publish" or "export" or "validate" or "preview" or
            "status" or "stop" or "cancel" or "resume"))
            throw new ArgumentException("usage: ud_cli run|publish|export|validate|preview|status|stop|cancel|resume JOB_ID [options]");

        string command = args[0];
        string jobId = args[1];
        if (string.IsNullOrWhiteSpace(jobId)) throw new ArgumentException("A job ID is required");
        string? config = null, parametersFile = null, output = null, requestId = null,
            expectedRevision = null, managementUrl = null, bearerToken = null, cookie = null, csrf = null,
            environmentId = null, environmentRevision = null, executionId = null;
        uint sourceRows = 1000, sampleRows = 32;
        bool run = false, help = false;
        var overrides = new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);

        for (int index = 2; index < args.Length; index++)
        {
            string argument = args[index];
            if (argument is "-h" or "--help") { help = true; continue; }
            if (TryOption(argument, "--run", out var inline))
            {
                if (inline is not null) throw new ArgumentException("--run does not take a value");
                run = true;
                continue;
            }
            if (TryOption(argument, "--config", out inline)) { config = ReadValue(args, ref index, inline, "--config"); continue; }
            if (TryOption(argument, "--params-file", out inline)) { parametersFile = ReadValue(args, ref index, inline, "--params-file"); continue; }
            if (TryOption(argument, "--output", out inline)) { output = ReadValue(args, ref index, inline, "--output"); continue; }
            if (TryOption(argument, "--request-id", out inline)) { requestId = ReadValue(args, ref index, inline, "--request-id"); continue; }
            if (TryOption(argument, "--expected-revision", out inline)) { expectedRevision = ReadValue(args, ref index, inline, "--expected-revision"); continue; }
            if (TryOption(argument, "--management-url", out inline) || TryOption(argument, "--base-url", out inline))
            { managementUrl = ReadValue(args, ref index, inline, "--management-url"); continue; }
            if (TryOption(argument, "--token", out inline) || TryOption(argument, "--bearer-token", out inline))
            { bearerToken = ReadValue(args, ref index, inline, "--token"); continue; }
            if (TryOption(argument, "--cookie", out inline)) { cookie = ReadValue(args, ref index, inline, "--cookie"); continue; }
            if (TryOption(argument, "--csrf", out inline)) { csrf = ReadValue(args, ref index, inline, "--csrf"); continue; }
            if (TryOption(argument, "--environment-id", out inline))
            { environmentId = ReadValue(args, ref index, inline, "--environment-id"); continue; }
            if (TryOption(argument, "--environment-revision", out inline))
            { environmentRevision = ReadValue(args, ref index, inline, "--environment-revision"); continue; }
            if (TryOption(argument, "--execution-id", out inline) || TryOption(argument, "--run-id", out inline))
            { executionId = ReadValue(args, ref index, inline, "--execution-id"); continue; }
            if (TryOption(argument, "--source-rows", out inline))
            { sourceRows = ParseUInt(ReadValue(args, ref index, inline, "--source-rows"), "--source-rows"); continue; }
            if (TryOption(argument, "--sample-rows", out inline))
            { sampleRows = ParseUInt(ReadValue(args, ref index, inline, "--sample-rows"), "--sample-rows"); continue; }
            if (TryOption(argument, "--set", out inline) || TryOption(argument, "--param", out inline) ||
                TryOption(argument, "--parameter", out inline))
            {
                string assignment = ReadValue(args, ref index, inline, "--set");
                int equals = assignment.IndexOf('=');
                if (equals <= 0) throw new ArgumentException("Parameter overrides must use name=value");
                string name = assignment[..equals].Trim();
                if (name.Length == 0 || !overrides.TryAdd(name,
                        DataConfiguration.ParseOverrideValue(assignment[(equals + 1)..].Trim())))
                    throw new ArgumentException($"Duplicate or empty parameter override '{name}'");
                continue;
            }
            throw new ArgumentException($"Unknown job command option '{argument}'");
        }

        if (command == "run" && run) throw new ArgumentException("--run is only valid with publish");
        if (command == "export" && (run || requestId is not null || expectedRevision is not null ||
            environmentId is not null || environmentRevision is not null))
            throw new ArgumentException("export does not accept publish options");
        if (environmentId is not null ^ environmentRevision is not null)
            throw new ArgumentException("--environment-id and --environment-revision must be supplied together");
        if (command is ("status" or "stop" or "cancel" or "resume") && string.IsNullOrWhiteSpace(executionId))
            executionId = jobId;
        if (sourceRows is 0 or > 10000 || sampleRows is 0 or > 32 || sampleRows > sourceRows)
            throw new ArgumentException("preview row limits are outside the Native bounds");
        return new(command, jobId, config, parametersFile, overrides, output, requestId,
            expectedRevision, managementUrl, bearerToken, cookie, csrf, environmentId, environmentRevision, executionId,
            sourceRows, sampleRows, run, help);
    }

    private static string ReadValue(string[] args, ref int index, string? inline, string option)
    {
        string value = inline ?? (++index < args.Length ? args[index] : throw new ArgumentException($"{option} requires a value"));
        if (string.IsNullOrWhiteSpace(value)) throw new ArgumentException($"{option} requires a non-empty value");
        return value;
    }

    private static bool TryOption(string argument, string option, out string? inline)
    {
        if (argument.Equals(option, StringComparison.Ordinal)) { inline = null; return true; }
        string prefix = option + "=";
        if (argument.StartsWith(prefix, StringComparison.Ordinal)) { inline = argument[prefix.Length..]; return true; }
        inline = null;
        return false;
    }

    private static uint ParseUInt(string value, string option)
    {
        if (!uint.TryParse(value, System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture, out var parsed))
            throw new ArgumentException($"{option} requires a non-negative integer");
        return parsed;
    }
}
