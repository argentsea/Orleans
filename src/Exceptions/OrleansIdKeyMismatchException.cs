// © John Hicks. All rights reserved. Licensed under the MIT license.
// See the LICENSE file in the repository root for more information.

using Orleans;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Text;

namespace ArgentSea.Orleans
{
    public sealed class OrleansIdKeyMismatchException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="OrleansIdKeyMismatchException" /> class with an error message.
        /// </summary>
        public OrleansIdKeyMismatchException()
            : base($"The Orleans Grain Id of a grain type does not match the key value of thew grain.")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ShardKeyNotFoundException" /> class with a specified error message.
        /// </summary>
        /// <param name="grainType">The type of the grain to show in the message.</param>
        public OrleansIdKeyMismatchException(string grainType)
            : base($"The Orleans Grain Id of a “{grainType}” grain type does not match the key value of thew grain.")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ShardKeyNotFoundException" /> class with a specified error message.
        /// </summary>
        /// <param name="grainType">The type of the grain to show in the message.</param>
        /// <param name="grainId">The ShardKey.ToString() result.</param>
        /// <param name="shardKey">The ShardKey.ToString() result.</param>
        public OrleansIdKeyMismatchException(string grainType, string grainId, string shardKey)
            : base($"The Orleans Grain Id ({ grainId }) of a “{grainType}” grain type does not match the key value ({ shardKey })of thew grain.")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ShardKeyNotFoundException" /> class with a specified error message.
        /// </summary>
        /// <param name="grainType">The type of the grain to show in the message.</param>
        /// <param name="grainId">The ShardKey.ToString() result.</param>
        /// <param name="shardKey">The ShardKey.ToString() result.</param>
        /// <param name="innerException">The exception that is the cause of the current exception.</param>
        public OrleansIdKeyMismatchException(string grainType, string grainId, string shardKey, Exception innerException)
            : base($"The Orleans Grain Id ({grainId}) of a “{grainType}” grain type does not match the key value ({shardKey})of thew grain.", innerException)
        {
        }

    }
}
