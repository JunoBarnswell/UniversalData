using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text.Json;

namespace UniversalData;

internal static class SdkBusinessRuntime
{
    internal const string BuiltinModule = "UniversalData.Sdk";

    internal static async Task<JsonElement> RunAsync(SdkInvocation invocation, SdkExecutionEnvironment environment,
        SdkModuleCatalog modules, CancellationToken cancellation)
    {
        if (!Guid.TryParseExact(invocation.RunId, "N", out _) || environment.MaxBranches < 1 ||
            environment.MaxConnectionsPerSource < 1 || invocation.Sources.Length > 4096 ||
            invocation.Sources.Select(source => source.Definition.Name).Distinct(StringComparer.Ordinal).Count() != invocation.Sources.Length)
            throw new InvalidDataException("Invalid SDK invocation");
        using var state = new SdkInvocationState(new(invocation.RunId), environment, invocation.Sources, invocation.DefaultSource);
        var context = new DataContext(state, cancellation);
        if (invocation.Method.ModuleSha256 == BuiltinModule && invocation.Method.Name == "probe")
        {
            string name = invocation.Argument?.GetString() ?? throw new InvalidDataException("Probe source is required");
            var probe = await context.InvokeAsync((ctx, ct) => ProbeAsync(ctx, name, ct)).ConfigureAwait(false);
            return JsonSerializer.SerializeToElement(probe, WorkerProtocol.Json);
        }

        modules.Bind(invocation.Modules);
        var method = modules.Resolve(invocation.Method);
        var parameters = method.GetParameters();
        bool withArgument = invocation.Method.HasArgument;
        if (!method.IsPublic || !method.IsStatic || method.IsGenericMethod ||
            parameters.Length != (withArgument ? 3 : 2) || parameters[0].ParameterType != typeof(DataContext) ||
            parameters[^1].ParameterType != typeof(CancellationToken) || !typeof(Task).IsAssignableFrom(method.ReturnType))
            throw new InvalidDataException("The resolved method does not match the business invocation contract");

        return await context.InvokeAsync(async (ctx, ct) =>
        {
            object? argument = withArgument
                ? (invocation.Argument ?? throw new InvalidDataException("Argument is missing"))
                    .Deserialize(parameters[1].ParameterType, WorkerProtocol.Json)
                : null;
            Task result;
            try
            {
                result = (Task)(method.Invoke(null, withArgument ? [ctx, argument, ct] : [ctx, ct])
                    ?? throw new InvalidDataException("Business method returned a null Task"));
            }
            catch (TargetInvocationException exception) when (exception.InnerException is { } cause)
            {
                ExceptionDispatchInfo.Capture(cause).Throw();
                throw;
            }
            await result.ConfigureAwait(false);
            if (!method.ReturnType.IsGenericType) return JsonSerializer.SerializeToElement<object?>(null, WorkerProtocol.Json);
            var value = DataDatabase.CheckResult(method.ReturnType.GetProperty("Result")!.GetValue(result));
            return JsonSerializer.SerializeToElement(value, method.ReturnType.GenericTypeArguments[0], WorkerProtocol.Json);
        }).ConfigureAwait(false);
    }

    internal static DataRecoveryResult Recover(SdkRecoveryInvocation invocation,
        SdkExecutionEnvironment environment, CancellationToken cancellation) =>
        SdkAtomicRuntime.Recover(invocation, environment, cancellation);

    private static async Task<DataSourceProbe> ProbeAsync(DataContext context, string name, CancellationToken cancellation)
    {
        var source = context.State.Sources.Single(binding => binding.Definition.Name == name);
        if (!source.Definition.IsDatabase)
        {
            var probe = await new DataHttp(context, source).ProbeAsync(cancellation).ConfigureAwait(false);
            return new(DataSourceRegistry.Describe(source), probe, probe ? ["HTTP"] : []);
        }
        try
        {
            bool reachable = await context.Database(name).ProbeAsync(cancellation).ConfigureAwait(false);
            return new(DataSourceRegistry.Describe(source), reachable, reachable ? ["SQL"] : []);
        }
        catch (System.Data.Common.DbException)
        {
            return new(DataSourceRegistry.Describe(source), false, []);
        }
    }
}
