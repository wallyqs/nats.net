using NATS.Client.TestUtilities;

namespace NATS.Client.Core.Tests;

/// <summary>
/// Tests for the scenario where the NATS server is configured with:
///   tls { ... }
///   allow_non_tls = true
///
/// In this case the server sends INFO with tls_available=true, tls_required=false.
/// The client should be able to connect using plain text when TlsMode.Disable is set.
/// </summary>
public class TlsAvailableNonTlsFallbackTest
{
    private readonly ITestOutputHelper _output;

    public TlsAvailableNonTlsFallbackTest(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task Client_connects_plain_text_when_tls_available_but_not_required()
    {
        // Simulate a server configured with: tls { ... } + allow_non_tls = true
        // Server sends tls_available=true, tls_required=false in INFO
        var info = """{"tls_available":true,"tls_required":false,"max_payload":1048576}""";

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await using var server = new MockServer(
            handler: (_, _) => Task.CompletedTask,
            logger: m => _output.WriteLine(m),
            info: info,
            cancellationToken: cts.Token);

        await using var nats = new NatsConnection(new NatsOpts
        {
            Url = server.Url,
            TlsOpts = new NatsTlsOpts
            {
                Mode = TlsMode.Disable,
            },
        });

        await nats.ConnectAsync();
        var rtt = await nats.PingAsync(cts.Token);

        _output.WriteLine($"Connected with plain text, RTT: {rtt}");

        // Verify server info was parsed correctly
        Assert.NotNull(nats.ServerInfo);
        Assert.True(nats.ServerInfo.TlsAvailable);
        Assert.False(nats.ServerInfo.TlsRequired);
    }

    [Fact]
    public async Task Client_upgrades_to_tls_by_default_when_tls_available()
    {
        // When using default TlsMode (Auto -> Prefer), the client will attempt
        // to upgrade to TLS when tls_available=true. Since our mock server
        // doesn't actually support TLS, this should fail.
        var info = """{"tls_available":true,"tls_required":false,"max_payload":1048576}""";

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await using var server = new MockServer(
            handler: (_, _) => Task.CompletedTask,
            logger: m => _output.WriteLine(m),
            info: info,
            cancellationToken: cts.Token);

        await using var nats = new NatsConnection(new NatsOpts
        {
            Url = server.Url,
            ConnectTimeout = TimeSpan.FromSeconds(5),
        });

        // Default Auto mode resolves to Prefer for nats:// without certs.
        // Prefer + tls_available=true => attempts TLS upgrade.
        // Mock server doesn't support TLS handshake, so this should fail.
        await Assert.ThrowsAnyAsync<Exception>(async () => await nats.ConnectAsync());
    }

    [Fact]
    public async Task Client_connects_plain_text_when_tls_not_available()
    {
        // When tls_available=false and tls_required=false, plain text is used
        // regardless of TlsMode (Auto/Prefer).
        var info = """{"tls_available":false,"tls_required":false,"max_payload":1048576}""";

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await using var server = new MockServer(
            handler: (_, _) => Task.CompletedTask,
            logger: m => _output.WriteLine(m),
            info: info,
            cancellationToken: cts.Token);

        await using var nats = new NatsConnection(new NatsOpts
        {
            Url = server.Url,
        });

        await nats.ConnectAsync();
        var rtt = await nats.PingAsync(cts.Token);

        _output.WriteLine($"Connected with plain text (no TLS available), RTT: {rtt}");

        Assert.NotNull(nats.ServerInfo);
        Assert.False(nats.ServerInfo.TlsAvailable);
        Assert.False(nats.ServerInfo.TlsRequired);
    }
}
