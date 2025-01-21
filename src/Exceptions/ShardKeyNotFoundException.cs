// © John Hicks. All rights reserved. Licensed under the MIT license.
// See the LICENSE file in the repository root for more information.

using System;
using System.Collections.Generic;
using System.Text;

namespace ArgentSea.Orleans
{
    public sealed class ShardKeyNotFoundException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ShardKeyNotFoundException" /> class with an error message.
        /// </summary>
        public ShardKeyNotFoundException()
            : base("The Orleans Grain cannot be serialize/deserialzied because the ShardKey attribute could not be found. This attribute is needed to invoke the correct database.")
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ShardKeyNotFoundException" /> class with a specified error message.
        /// </summary>
        /// <param name="grainType">The type of the grain to show in the message.</param>
        public ShardKeyNotFoundException(string grainType)
            : base($"The Orleans Grain “{grainType}” cannot be serialize/deserialzied because a ShardKey attribute could not be found. This attribute is needed to invoke the correct database.")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ShardKeyNotFoundException" /> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        /// <param name="innerException">The exception that is the cause of the current exception.</param>
        public ShardKeyNotFoundException(string grainType, Exception innerException)
            : base($"The Orleans Grain “{grainType}” cannot be serialize/deserialzied because a ShardKey attribute could not be found. This attribute is needed to invoke the correct database.", innerException)
        {
        }
    }
}
