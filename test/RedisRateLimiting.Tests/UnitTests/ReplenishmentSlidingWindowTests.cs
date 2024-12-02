using System.Threading.RateLimiting;
using Xunit;

namespace RedisRateLimiting.Tests.UnitTests;

public class ReplenishmentSlidingWindowTests(TestFixture fixture) : IClassFixture<TestFixture>
{
    private readonly TestFixture Fixture = fixture;

    [Fact]
    public async Task LeaseOnlyAvailableAfterManualReplenishment()
    {
        using var limiter = new RedisReplenishmentSlidingWindowLimiter<string>(
            partitionKey: Guid.NewGuid().ToString(),
            new RedisReplenishmentSlidingWindowRateLimiterOptions()
            {
                PermitLimit = 1,
                Window = TimeSpan.FromMilliseconds(600),
                ConnectionMultiplexerFactory = Fixture.ConnectionMultiplexerFactory
            });

        using var lease = await limiter.AcquireAsync();
        lease.TryGetMetadata(RateLimitMetadataName.RequestId.Name, out var requestId);
        Assert.True(lease.IsAcquired);

        //not available before window expires
        using var lease2 = await limiter.AcquireAsync();
        lease2.TryGetMetadata(RateLimitMetadataName.RequestId.Name, out var requestId2);
        Assert.False(lease2.IsAcquired);
        
        limiter.TryReplenish((string)requestId);
            
        using var lease3 = await limiter.AcquireAsync();
        lease3.TryGetMetadata(RateLimitMetadataName.RequestId.Name, out var requestId3);
        Assert.False(lease3.IsAcquired);
    }

    [Fact]
    public async Task LeaseUnavailableBeforeWindowOpens()
    {
        using var limiter = new RedisReplenishmentSlidingWindowLimiter<string>(
            partitionKey: Guid.NewGuid().ToString(),
            new RedisReplenishmentSlidingWindowRateLimiterOptions()
            {
                PermitLimit = 1,
                Window = TimeSpan.FromMilliseconds(600),
                ConnectionMultiplexerFactory = Fixture.ConnectionMultiplexerFactory
            });

        using var lease = await limiter.AcquireAsync();
        lease.TryGetMetadata(RateLimitMetadataName.RequestId.Name, out var requestId);
        Assert.True(lease.IsAcquired);

        limiter.TryReplenish((string)requestId);
        
        using var lease2 = await limiter.AcquireAsync();
        lease2.TryGetMetadata(RateLimitMetadataName.RequestId.Name, out var requestId2);
        Assert.False(lease2.IsAcquired);

        await Task.Delay(TimeSpan.FromMilliseconds(600));
            
        using var lease3 = await limiter.AcquireAsync();
        lease3.TryGetMetadata(RateLimitMetadataName.RequestId.Name, out var requestId3);
        Assert.True(lease3.IsAcquired);
    }
    
      [Fact]
        public void InvalidOptionsThrows()
        {
            AssertExtensions.Throws<ArgumentNullException>("options", () => new RedisReplenishmentSlidingWindowLimiter<string>(
                string.Empty,
                options: null));

            AssertExtensions.Throws<ArgumentException>("options", () => new RedisReplenishmentSlidingWindowLimiter<string>(
                string.Empty,
                new RedisReplenishmentSlidingWindowRateLimiterOptions
                {
                    PermitLimit = -1,
                }));

            AssertExtensions.Throws<ArgumentException>("options", () => new RedisReplenishmentSlidingWindowLimiter<string>(
               string.Empty,
               new RedisReplenishmentSlidingWindowRateLimiterOptions
               {
                   PermitLimit = 1,
               }));

            AssertExtensions.Throws<ArgumentException>("options", () => new RedisReplenishmentSlidingWindowLimiter<string>(
                string.Empty,
                new RedisReplenishmentSlidingWindowRateLimiterOptions
                {
                    PermitLimit = 1,
                    Window = TimeSpan.Zero,
                }));

            AssertExtensions.Throws<ArgumentException>("options", () => new RedisReplenishmentSlidingWindowLimiter<string>(
                string.Empty,
                new RedisReplenishmentSlidingWindowRateLimiterOptions
                {
                    PermitLimit = 1,
                    Window = TimeSpan.FromMinutes(-1),
                }));

            AssertExtensions.Throws<ArgumentException>("options", () => new RedisReplenishmentSlidingWindowLimiter<string>(
                string.Empty,
                new RedisReplenishmentSlidingWindowRateLimiterOptions
                {
                    PermitLimit = 1,
                    Window = TimeSpan.FromMinutes(1),
                    ConnectionMultiplexerFactory = null,
                }));
        }

        [Fact]
        public async Task ThrowsWhenAcquiringMoreThanLimit()
        {
            var limiter = new RedisReplenishmentSlidingWindowLimiter<string>(
                partitionKey: Guid.NewGuid().ToString(),
                new RedisReplenishmentSlidingWindowRateLimiterOptions()
                {
                    PermitLimit = 1,
                    Window = TimeSpan.FromMinutes(1),
                    ConnectionMultiplexerFactory = Fixture.ConnectionMultiplexerFactory,
                });
            var ex = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await limiter.AcquireAsync(2));
            Assert.Equal("permitCount", ex.ParamName);
        }

        [Fact]
        public async Task CanAcquireAsyncResource()
        {
            using var limiter = new RedisReplenishmentSlidingWindowLimiter<string>(
                partitionKey: Guid.NewGuid().ToString(),
                new RedisReplenishmentSlidingWindowRateLimiterOptions
                {
                    PermitLimit = 1,
                    Window = TimeSpan.FromMinutes(1),
                    ConnectionMultiplexerFactory = Fixture.ConnectionMultiplexerFactory,
                });

            using var lease = await limiter.AcquireAsync();
            Assert.True(lease.IsAcquired);

            using var lease2 = await limiter.AcquireAsync();
            Assert.False(lease2.IsAcquired);

            var stats = limiter.GetStatistics()!;
            Assert.Equal(1, stats.TotalSuccessfulLeases);
            Assert.Equal(1, stats.TotalFailedLeases);
            Assert.Equal(0, stats.CurrentAvailablePermits);

            lease.Dispose();
            lease2.Dispose();

            stats = limiter.GetStatistics()!;
            Assert.Equal(0, stats.CurrentAvailablePermits);
        }

        [Fact]
        public async Task CanAcquireAsyncResourceWithSmallWindow()
        {
            using var limiter = new RedisReplenishmentSlidingWindowLimiter<string>(
                partitionKey: Guid.NewGuid().ToString(),
                new RedisReplenishmentSlidingWindowRateLimiterOptions
                {
                    PermitLimit = 1,
                    Window = TimeSpan.FromMilliseconds(600),
                    ConnectionMultiplexerFactory = Fixture.ConnectionMultiplexerFactory,
                });

            using var lease = await limiter.AcquireAsync();
            Assert.True(lease.IsAcquired);

            using var lease2 = await limiter.AcquireAsync();
            Assert.False(lease2.IsAcquired);

            await Task.Delay(TimeSpan.FromMilliseconds(600));

            using var lease3 = await limiter.AcquireAsync();
            Assert.True(lease3.IsAcquired);

            using var lease4 = await limiter.AcquireAsync();
            Assert.False(lease4.IsAcquired);
        }
        
        [Fact]
        public async Task IdleDurationIsUpdated()
        {
            await using var limiter = new RedisReplenishmentSlidingWindowLimiter<string>(
                partitionKey: Guid.NewGuid().ToString(),
                new RedisReplenishmentSlidingWindowRateLimiterOptions
                {
                    PermitLimit = 1,
                    Window = TimeSpan.FromMilliseconds(600),
                    ConnectionMultiplexerFactory = Fixture.ConnectionMultiplexerFactory,
                });
            await Task.Delay(TimeSpan.FromMilliseconds(5));
            Assert.NotEqual(TimeSpan.Zero, limiter.IdleDuration);

            var previousIdleDuration = limiter.IdleDuration;
            using var lease = await limiter.AcquireAsync();
            Assert.True(limiter.IdleDuration < previousIdleDuration);
        }

}