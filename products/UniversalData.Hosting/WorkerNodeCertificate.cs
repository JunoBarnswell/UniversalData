using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace UniversalData.Hosting;

public static class WorkerNodeCertificate
{
    public static bool ValidPin(string? pin) => pin is { Length: 64 } && pin.All(Uri.IsHexDigit);

    public static bool Matches(X509Certificate2 certificate, string pin) =>
        ValidPin(pin) && CryptographicOperations.FixedTimeEquals(
            certificate.GetCertHash(HashAlgorithmName.SHA256), Convert.FromHexString(pin));
}
