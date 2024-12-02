using System;
using System.Formats.Asn1;
using System.Threading.RateLimiting;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace RedisRateLimiting.Concurrency;

internal class RedisReplenishmentSlidingWindowManager
{
    private readonly IConnectionMultiplexer _connectionMultiplexer;
    private readonly RedisReplenishmentSlidingWindowRateLimiterOptions _options;
    private readonly RedisKey RateLimitWindowKey;
    private readonly RedisKey RateLimitReplenishmentKey;
    private readonly RedisKey StatsRateLimitKey;

    private static readonly LuaScript AcquireScript = LuaScript.Prepare(
        @"local limit = tonumber(@permit_limit)
            local timestamp = tonumber(@current_time)
            local window = tonumber(@window)

            -- remove all requests outside current window
            redis.call(""zremrangebyscore"", @rate_limit_key_window, '-inf', timestamp - window)

            local window_count = redis.call(""zcard"", @rate_limit_key_window)
            local replenish_count = redis.call(""zcard"", @rate_limit_key_replenish)

            local allowed = window_count < limit and replenish_count < limit

            if allowed
            then
                redis.call(""zadd"", @rate_limit_key_window, timestamp, @unique_id)
                redis.call(""zadd"", @rate_limit_key_replenish, timestamp, @unique_id)
            end

            -- ratelimit expiration
            local expireAtMilliseconds = math.floor((timestamp + window) * 1000 + 1);
            redis.call(""pexpireat"", @rate_limit_key_window, expireAtMilliseconds)
            
            -- replenishment absolute expiration
            redis.call(""pexpireat"", @rate_limit_key_replenish, @replenish_timeout_ms)

            if allowed
            then
                redis.call(""hincrby"", @stats_key, 'total_successful', 1)
            else
                redis.call(""hincrby"", @stats_key, 'total_failed', 1)
            end

            return { allowed, window_count }");

    private static readonly LuaScript ReplenishScript = LuaScript.Prepare(
        @"local removed_count = redis.call(""ZREM"", @rate_limit_key_replenish, @unique_id)
            return { removed_count}");

    private static readonly LuaScript StatisticsScript = LuaScript.Prepare(
        @"local count = redis.call(""zcard"", @rate_limit_key_window)
            local total_successful_count = redis.call(""hget"", @stats_key, 'total_successful')
            local total_failed_count = redis.call(""hget"", @stats_key, 'total_failed')
            return { count, total_successful_count, total_failed_count }");

    public RedisReplenishmentSlidingWindowManager(
        string partitionKey,
        RedisReplenishmentSlidingWindowRateLimiterOptions options)
    {
        _options = options;
        _connectionMultiplexer = options.ConnectionMultiplexerFactory!.Invoke();
        RateLimitWindowKey = new RedisKey($"rl:sw:{{{partitionKey}}}:window");
        RateLimitReplenishmentKey = new RedisKey($"rl:sw:{{{partitionKey}}}:replenishment");
        StatsRateLimitKey = new RedisKey($"rl:sw:{{{partitionKey}}}:stats");
    }


    internal async Task<RedisReplenishmentSlidingWindowResponse> TryAcquireLeaseAsync(string requestId)
    {
        var now = DateTimeOffset.UtcNow;
        double nowUnixTimeSeconds = now.ToUnixTimeMilliseconds() / 1000.0;

        var database = _connectionMultiplexer.GetDatabase();

        var response = (RedisValue[]?)await database.ScriptEvaluateAsync(
            AcquireScript,
            new
            {
                rate_limit_key_window = RateLimitWindowKey,
                rate_limit_key_replenish = RateLimitReplenishmentKey,
                stats_key = StatsRateLimitKey,
                permit_limit = (RedisValue)_options.PermitLimit,
                window = (RedisValue)_options.Window.TotalSeconds,
                current_time = (RedisValue)nowUnixTimeSeconds,
                unique_id = (RedisValue)requestId,
                replenish_timeout_ms = (RedisValue)_options.ReplenishmentAbsoluteTimeout.TotalMilliseconds,
            });

        var result = new RedisReplenishmentSlidingWindowResponse();

        if (response != null)
        {
            result.Allowed = (bool)response[0];
            result.Count = (long)response[1];
        }

        return result;
    }

    internal RedisReplenishmentSlidingWindowResponse TryAcquireLease(string requestId)
    {
        var now = DateTimeOffset.UtcNow;
        double nowUnixTimeSeconds = now.ToUnixTimeMilliseconds() / 1000.0;

        var database = _connectionMultiplexer.GetDatabase();

        var response = (RedisValue[]?)database.ScriptEvaluate(
            AcquireScript,
            new
            {
                rate_limit_key_window = RateLimitWindowKey,
                rate_limit_key_replenish = RateLimitReplenishmentKey,
                stats_key = StatsRateLimitKey,
                permit_limit = (RedisValue)_options.PermitLimit,
                window = (RedisValue)_options.Window.TotalSeconds,
                current_time = (RedisValue)nowUnixTimeSeconds,
                unique_id = (RedisValue)requestId,
                replenish_timeout_ms = (RedisValue)_options.ReplenishmentAbsoluteTimeout.TotalMilliseconds,
            });

        var result = new RedisReplenishmentSlidingWindowResponse();

        if (response != null)
        {
            result.Allowed = (bool)response[0];
            result.Count = (long)response[1];
        }

        return result;
    }

    internal RedisReplenishmentSlidingWindowReplenishResponse TryReplenish(string requestId)
    {
        var database = _connectionMultiplexer.GetDatabase();
        var response = (RedisValue[]?)database.ScriptEvaluate(
            ReplenishScript,
            new
            {
                rate_limit_key_replenish = RateLimitReplenishmentKey,
                unique_id = (RedisValue)requestId,
            });

        var result = new RedisReplenishmentSlidingWindowReplenishResponse();

        if (response != null)
        {
            result.RemovedCount = (long)response[0];
        }

        return result;
    }

    internal RateLimiterStatistics? GetStatistics()
    {
        var database = _connectionMultiplexer.GetDatabase();

        var response = (RedisValue[]?)database.ScriptEvaluate(
            StatisticsScript,
            new
            {
                rate_limit_key_window = RateLimitWindowKey,
                stats_key = StatsRateLimitKey,
            });

        if (response == null)
        {
            return null;
        }

        return new RateLimiterStatistics
        {
            CurrentAvailablePermits = _options.PermitLimit - (long)response[0],
            TotalSuccessfulLeases = (long)response[1],
            TotalFailedLeases = (long)response[2],
        };
    }
}

internal class RedisReplenishmentSlidingWindowReplenishResponse
{
    internal long RemovedCount { get; set; }
}

internal class RedisReplenishmentSlidingWindowResponse
{
    internal bool Allowed { get; set; }
    internal long Count { get; set; }
}