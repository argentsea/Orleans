// © John Hicks. All rights reserved. Licensed under the MIT license.
// See the LICENSE file in the repository root for more information.

using Orleans.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace ArgentSea.Orleans
{
    public class ArgentSeaGrainPersistenceException : Exception
    {
        /// <summary>
        /// Initializes a new instance of <see cref="ArgentSeaGrainPersistenceException"/>.
        /// </summary>
        public ArgentSeaGrainPersistenceException()
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ArgentSeaGrainPersistenceException"/>.
        /// </summary>
        /// <param name="message">The error message that explains the reason for the exception.</param>
        public ArgentSeaGrainPersistenceException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ArgentSeaGrainPersistenceException"/>.
        /// </summary>
        /// <param name="message">The error message that explains the reason for the exception.</param>
        /// <param name="inner">The exception that is the cause of the current exception, or a null reference (Nothing in Visual Basic) if no inner exception is specified.</param>
        public ArgentSeaGrainPersistenceException(string message, Exception inner) : base(message, inner)
        {
        }

        /// <inheritdoc />
        [Obsolete]
        protected ArgentSeaGrainPersistenceException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}
