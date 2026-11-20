using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace UniversalData;

/// <summary>Uses the authenticated management API that Studio uses. Configure credentials on the supplied HttpClient.</summary>
public sealed class EtlManagementClient(HttpClient http, string? csrf = null)
{
    public ValueTask<Result<JsonElement>> PublishAsync(string id, ulong revision, Package package,
        CancellationToken cancellation = default)
    {
        using var document = JsonDocument.Parse(package.ToJson());
        return Send<JsonElement>(HttpMethod.Put, $"/api/packages/{Segment(id)}/versions/{revision}",
            document.RootElement.Clone(), cancellation);
    }

    public ValueTask<Result<JsonElement>> SaveJobAsync(JobDefinition job, CancellationToken cancellation = default) =>
        Send<JsonElement>(HttpMethod.Put, $"/api/jobs/{Segment(job.Id)}", job, cancellation);

    public ValueTask<Result<long[]>> PreviewScheduleAsync(string job, JobSchedule schedule,
        CancellationToken cancellation = default) =>
        Send<long[]>(HttpMethod.Post, $"/api/jobs/{Segment(job)}/schedule-preview", schedule, cancellation);

    public ValueTask<Result<JobOccurrence>> RunJobAsync(string job, string requestId,
        string? occurrence = null, CancellationToken cancellation = default) =>
        Send<JobOccurrence>(HttpMethod.Post, $"/api/jobs/{Segment(job)}/runs",
            new { requestId, occurrenceId = occurrence }, cancellation);

    public ValueTask<Result<JobOccurrence[]>> JobHistoryAsync(string job, string? afterId = null,
        CancellationToken cancellation = default) =>
        Send<JobOccurrence[]>(HttpMethod.Get, $"/api/jobs/{Segment(job)}/occurrences" + (afterId is null ? "" : "?afterId=" + Segment(afterId)),
            null, cancellation);

    public ValueTask<Result<WorkerNodeStatus[]>> WorkersAsync(CancellationToken cancellation = default) =>
        Send<WorkerNodeStatus[]>(HttpMethod.Get, "/api/workers", null, cancellation);

    private async ValueTask<Result<T>> Send<T>(HttpMethod method, string path, object? body,
        CancellationToken cancellation)
    {
        try
        {
            using var request = new HttpRequestMessage(method, path);
            if (csrf is not null)
                request.Headers.Add("X-UD-CSRF", csrf);
            if (body is not null)
                request.Content = JsonContent.Create(body, options: WorkerProtocol.Json);
            using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellation)
                .ConfigureAwait(false);
            if (response.IsSuccessStatusCode)
            {
                var value = await response.Content.ReadFromJsonAsync<T>(WorkerProtocol.Json, cancellation)
                    .ConfigureAwait(false);
                return new(StatusCode.Ok, value);
            }
            int status = (int)response.StatusCode;
            Result? failure = null;
            try { failure = await response.Content.ReadFromJsonAsync<Result>(WorkerProtocol.Json, cancellation)
                    .ConfigureAwait(false); }
            catch (JsonException) { }
            if (failure is { IsSuccess: false } result)
                return new(result.Code, default, result.Error is null ? null : result.Error with { HttpStatus = status });
            var code = response.StatusCode is HttpStatusCode.Forbidden or HttpStatusCode.Unauthorized
                ? StatusCode.Auth : StatusCode.HttpStatus;
            return new(code, default, new(ErrorSource.Http, code, "Management request rejected", HttpStatus: status));
        }
        catch (OperationCanceledException)
        {
            return new(StatusCode.Cancelled, default, new(ErrorSource.Transport, StatusCode.Cancelled, "Management request cancelled"));
        }
        catch (Exception error) when (error is HttpRequestException or IOException or JsonException or InvalidOperationException)
        {
            var status = error is JsonException ? StatusCode.Protocol : StatusCode.Io;
            return new(status, default, new(ErrorSource.Transport, status, "Management service unavailable"));
        }
    }

    private static string Segment(string value) => Uri.EscapeDataString(value);
}
