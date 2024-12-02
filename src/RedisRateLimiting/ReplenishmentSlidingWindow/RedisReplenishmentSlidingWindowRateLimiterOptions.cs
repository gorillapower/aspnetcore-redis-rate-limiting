using System;

namespace RedisRateLimiting
{
    /// <summary>
    /// Options to specify the behavior of a <see cref="RedisSlidingWindowRateLimiterOptions"/>.
    /// </summary>
    public sealed class RedisReplenishmentSlidingWindowRateLimiterOptions : RedisRateLimiterOptions
    {
        /// <summary>
        /// Specifies the time window that takes in the requests.
        /// Must be set to a value greater than <see cref="TimeSpan.Zero" /> by the time these options are passed to the constructor of <see cref="RedisSlidingWindowRateLimiter{TKey}"/>.
        /// </summary>
        public TimeSpan Window { get; set; } = TimeSpan.Zero;

        /// <summary>
        /// Specifies the time interval after which the rate limiter will replenish the permits.
        /// </summary>
        public TimeSpan ReplenishmentAbsoluteTimeout { get; set; } = TimeSpan.FromSeconds(100);
        
        /// <summary>
        /// Specifies the time interval after which the rate limiter will try and replenish
        /// </summary>
        public TimeSpan ReplenishmentPeriod { get; set; } = TimeSpan.FromSeconds(100);

        /// <summary>
        /// Maximum number of permit counters that can be allowed in a window.
        /// Must be set to a value > 0 by the time these options are passed to the constructor of <see cref="RedisSlidingWindowRateLimiter{TKey}"/>.
        /// </summary>
        public int PermitLimit { get; set; }
    }
}
