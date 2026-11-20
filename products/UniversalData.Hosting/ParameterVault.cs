using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>Catalog contains only opaque references. A Worker resolves one immutable parameter set in its authorized scope.</summary>
public sealed class ParameterVault(HostPolicy policy)
{
    public string Store(string json)
    {
        using var document = JsonDocument.Parse(json);
        if (document.RootElement.ValueKind != JsonValueKind.Object || json.Length > 8 * 1024 * 1024) throw new ArgumentException("Invalid parameter set");
        string reference = "parameters-" + Guid.NewGuid().ToString("N");
        if (policy.Cluster is { } cluster)
        {
            cluster.Write(policy.Tenant, "protected/" + reference, json);
            return reference;
        }
        byte[] key = RandomNumberGenerator.GetBytes(32), nonce = RandomNumberGenerator.GetBytes(12), plaintext = Encoding.UTF8.GetBytes(json);
        byte[] ciphertext = new byte[plaintext.Length], tag = new byte[16];
        try {
            using var cipher = new AesGcm(key, 16); cipher.Encrypt(nonce, plaintext, ciphertext, tag, Binding(reference));
            var directory = DirectoryFor(reference);
            // Publish the reference only after both files have been flushed. A failed creation is never referenced.
            HostFiles.WritePrivate(Path.Combine(directory, "key"), key);
            HostFiles.WritePrivate(Path.Combine(directory, "payload"), nonce.Concat(tag).Concat(ciphertext).ToArray());
            return reference;
        } finally { CryptographicOperations.ZeroMemory(key); CryptographicOperations.ZeroMemory(plaintext); }
    }
    public string Resolve(string reference)
    {
        ValidateReference(reference);
        if (policy.Cluster is { } cluster)
            return cluster.Read(policy.Tenant, "protected/" + reference).Value
                ?? throw new IOException("Protected parameter set is unavailable");
        var directory = DirectoryFor(reference); var keyPath = Path.Combine(directory, "key"); var payloadPath = Path.Combine(directory, "payload");
        HostFiles.NoLinks(keyPath); HostFiles.NoLinks(payloadPath);
        byte[] key = File.ReadAllBytes(keyPath), payload = File.ReadAllBytes(payloadPath);
        if (key.Length != 32 || payload.Length is < 28 or > 8388636) throw new InvalidDataException("Protected parameter set is invalid");
        byte[] plaintext = new byte[payload.Length - 28];
        try {
            using var cipher = new AesGcm(key, 16); cipher.Decrypt(payload.AsSpan(0,12), payload.AsSpan(28), payload.AsSpan(12,16), plaintext, Binding(reference));
            return Encoding.UTF8.GetString(plaintext);
        } finally { CryptographicOperations.ZeroMemory(key); CryptographicOperations.ZeroMemory(plaintext); }
    }
    public string DirectoryFor(string reference)
    {
        ValidateReference(reference);
        return Path.Combine(policy.DataDirectory, "credentials", reference);
    }
    internal void RemoveLocal(string reference)
    {
        if (reference.Length == 0) return;
        var path = DirectoryFor(reference);
        try
        {
            File.Delete(Path.Combine(path,"key"));
            File.Delete(Path.Combine(path,"payload"));
            Directory.Delete(path);
        }
        catch (IOException) { }
    }
    private static void ValidateReference(string reference)
    {
        if (!reference.StartsWith("parameters-", StringComparison.Ordinal) || !Guid.TryParseExact(reference[11..], "N", out _))
            throw new ArgumentException("Invalid parameter credential reference");
    }
    private byte[] Binding(string reference) => Encoding.UTF8.GetBytes($"UniversalData/14/{policy.Tenant}/{reference}");
}
