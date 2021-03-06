using System;
using System.Runtime.InteropServices;
using System.Threading;
using EventStore.Core.Messaging;

namespace EventStore.Core.Bus {
	/// <summary>
	/// A much better concurrent queue than <see cref="System.Collections.Concurrent.ConcurrentQueue{T}"/> for multi producer single consumer scenarios.
	/// </summary>
	[StructLayout(LayoutKind.Explicit)]
	// ReSharper disable once InconsistentNaming
	public sealed class MPSCMessageQueue : ISingleConsumerMessageQueue {
		const int CacheLineSize = 64;
		const int Padding = CacheLineSize;
		const int MinimalSize = 1 << MinimalSizeLog;
		const int MinimalSizeLog = MaskShift * 2;
		const int MaskShift = 8;
		const int MaskLow = 0x000000FF;
		const int MaskHigh = 0x0000FF00;
		const int MaskUnchanged = ~0 ^ (MaskLow | MaskHigh);

		private struct MessageItem {
			public volatile Message Item;
		}

		[FieldOffset(Padding + CacheLineSize * 0)]
		private readonly MessageItem[] array;

		[FieldOffset(Padding + CacheLineSize * 1)]
		private readonly int count;

		[FieldOffset(Padding + CacheLineSize * 2)]
		private long sequence;

		[FieldOffset(Padding + CacheLineSize * 3)]
		private volatile IntPtr sequenceReadTo;

		[FieldOffset(Padding + CacheLineSize * 4)]
		private volatile IntPtr sequenceReadToCache;

		[FieldOffset(Padding + CacheLineSize * 5)]
		private long sequenceReadToValue;

		[FieldOffset(Padding + CacheLineSize * 6)]
		public long padding;

		public MPSCMessageQueue(int size) {
			if (IntPtr.Size != 8) {
				throw new NotSupportedException(
					"This queue is supported only on architectures having IntPtr.Size equal to 8");
			}

			if (IsPowerOf2(size) == false) {
				throw new ArgumentException("Use only sizes equal power of 2");
			}

			if (size < MinimalSize) {
				throw new ArgumentException("The size should be at least " + MinimalSize);
			}

			array = new MessageItem[size];
			count = size;
			sequence = 0;
			sequenceReadTo = IntPtr.Zero;
			sequenceReadToCache = IntPtr.Zero;
			sequenceReadToValue = 0;
		}

		public unsafe void Enqueue(Message item) {
			var next = Interlocked.Increment(ref sequence) - 1;
			var index = Map((int)(next & (count - 1)));

			do {
				// Volatile.Read(ref sequenceReadToCache);
				var srtc = sequenceReadToCache;
				var readTo = *(long*)&srtc;

				if (next - readTo < count) {
					array[index].Item = item;
					return;
				}

				// Volatile.Read(ref sequenceReadTo);
				var srt = sequenceReadTo;
				readTo = *(long*)&srt;

				// Volatile.Write(ref sequenceReadToCache, readTo);
				sequenceReadToCache = srt;

				if (next - readTo < count) {
					// Volatile.Write(ref array[index], item);
					array[index].Item = item;
					return;
				}
			} while (true);
		}

		public unsafe bool TryDequeue(Message[] segment, out QueueBatchDequeueResult result) {
			var i = 0;
			var length = segment.Length;

			// only one reader, no need to Volatile.Read(ref sequenceReadTo);
			// To do not get volatile read, the value is stored in a separate field and then persisted in both the sequenceReadToValue and sequenceRead.
			var current = sequenceReadToValue;

			while (i < length) {
				var index = Map((int)(current & (count - 1)));
				var stored = array[index].Item;
				if (stored != null) {
					segment[i] = stored;

					// if Volatile.Write was available, nulls could be assigned without it as the sequenceReadTo is writtent with volatile;
					array[index].Item = null;
					i += 1;
					current += 1;
				} else {
					break;
				}
			}

			if (i == 0) {
				result = default(QueueBatchDequeueResult);
				return false;
			}

			var currentSequence = Interlocked.CompareExchange(ref sequence, 0, 0);
			var estimatedCount = currentSequence - current;

			sequenceReadToValue = current;
			var c = *(IntPtr*)&current;

			// Volatile.Write(ref sequenceReadTo, current);
			sequenceReadTo = c;

			result = new QueueBatchDequeueResult {
				DequeueCount = i,
				EstimateCurrentQueueCount = (int)estimatedCount
			};

			return true;
		}

		public int EstimageCurrentQueueCount() {
			var currentSequence = Interlocked.CompareExchange(ref sequence, 0, 0);
			return (int)(currentSequence - sequenceReadToValue);
		}

		private static int Map(int s) {
			var low = s & MaskLow;
			var high = s & MaskHigh;
			var unchanged = s & MaskUnchanged;

			return (low << MaskShift) | high >> MaskShift | unchanged;
		}

		private static bool IsPowerOf2(int n) {
			return (n & (n - 1)) == 0;
		}
	}
}
