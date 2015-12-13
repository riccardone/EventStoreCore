using System;

namespace EventStore.Core.Services.Transport.Amqp.Lite
{
    public interface IBufferManager
    {
        /// <summary>
        /// Takes a buffer from the buffer manager.
        /// </summary>
        /// <param name="bufferSize">the buffer size.</param>
        /// <returns>
        /// A segment of a byte array. The count should be the same, or larger
        /// than the requested bufferSize.
        /// </returns>
        ArraySegment<byte> TakeBuffer(int bufferSize);

        /// <summary>
        /// Returns a buffer to the buffer manager.
        /// </summary>
        /// <param name="buffer">The buffer to return.</param>
        void ReturnBuffer(ArraySegment<byte> buffer);
    }
}
