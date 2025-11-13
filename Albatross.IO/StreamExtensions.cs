using Microsoft.Extensions.Logging;
using Polly;
using System;
using System.IO;
using System.Threading.Tasks;
using System.Threading;
using Polly.Retry;

namespace Albatross.IO {
	public static class StreamExtensions {
		public static ValueTask<Stream> OpenSharedReadStreamWithRetry(this FileInfo file, int bufferSize, int retryCount, TimeSpan delay, FileOptions fileOptions, ILogger logger, Action<int>? onRetry = null, CancellationToken cancellationToken = default) {
			var policy = CreateRetryPolicy(retryCount, delay, onRetry, logger);
			var context = ResilienceContextPool.Shared.Get(file.FullName, cancellationToken);
			try {
				context.Properties.Set(new ResiliencePropertyKey<string>("action"), "open-async-shared-read");
				return policy.ExecuteAsync<Stream, ResilienceContext>((ctx, token) => {
					Stream stream = new FileStream(file.FullName, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, fileOptions);
					return new ValueTask<Stream>(stream);
				}, context, cancellationToken);
			} finally {
				ResilienceContextPool.Shared.Return(context);
			}
		}

		public static ValueTask<Stream> OpenExclusiveReadWriteStreamWithRetry(this FileInfo file, int bufferSize, int retryCount, TimeSpan delay, FileOptions fileOptions, ILogger logger, Action<int>? onRetry = null, CancellationToken cancellationToken = default) {
			var policy = CreateRetryPolicy(retryCount, delay, onRetry, logger);
			var context = ResilienceContextPool.Shared.Get(file.FullName, cancellationToken);
			try {
				context.Properties.Set(new ResiliencePropertyKey<string>("action"), "open-async-exclusive-readwrite");
				return policy.ExecuteAsync<Stream, ResilienceContext>((ctx, token) => {
					Stream stream = new FileStream(file.FullName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, bufferSize, fileOptions);
					return new ValueTask<Stream>(stream);
				}, context, cancellationToken);
			} finally {
				ResilienceContextPool.Shared.Return(context);
			}
		}

		const int ErrorSharingViolation = unchecked((int)0x80070020);

		static ResiliencePipeline<Stream> CreateRetryPolicy(int count, TimeSpan delay, Action<int>? action, ILogger logger) =>
			new ResiliencePipelineBuilder<Stream>()
				.AddRetry(new RetryStrategyOptions<Stream> {
					MaxRetryAttempts = count,
					Delay = delay,
					BackoffType = DelayBackoffType.Constant,
					ShouldHandle = new PredicateBuilder<Stream>().Handle<IOException>(static err => err is not FileNotFoundException 
					                                                                                && err is not DirectoryNotFoundException 
					                                                                                && err is not PathTooLongException),
					OnRetry = args => {
						if (action != null) {
							action(args.AttemptNumber);
						}
						logger.LogWarning("{attempt} retry to open file {name} for {action} after {delay:#,#}ms",
							args.AttemptNumber, args.Context.OperationKey, args.Context.Properties.GetValue(new ResiliencePropertyKey<string>("action"), "unknown action"), args.RetryDelay.TotalMilliseconds);
						return default; // required because OnRetry is ValueTask
					}
				}).Build();
	}
}