// © John Hicks. All rights reserved. Licensed under the MIT license.
// See the LICENSE file in the repository root for more information.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace ArgentSea.Orleans
{
    public sealed class OrleansQueryNotProvidedException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QueryMapMissingException" /> class with an error message.
        /// </summary>
        public OrleansQueryNotProvidedException([CallerMemberName] string callerMethod = "")
            : base($"The current Orleans Grain does not have a query defined for its {callerMethod.Replace("StateAsync", "") } method.")
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryMapMissingException" /> class with a specified error message.
        /// </summary>
        /// <param name="grainType">The type of the grain to show in the message.</param>
        public OrleansQueryNotProvidedException(string grainType, [CallerMemberName] string callerMethod = "")
            : base($"The Orleans Grain “{ grainType }” does not have a query defined for its { callerMethod.Replace("StateAsync", "") } method.")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryMapMissingException" /> class.
        /// </summary>
        /// <param name="grainType">The type of the grain to show in the message.</param>
        /// <param name="innerException">The exception that is the cause of the current exception.</param>
        public OrleansQueryNotProvidedException(string grainType, Exception innerException, [CallerMemberName] string callerMethod = "")
            : base($"The Orleans Grain “{grainType}” does not have a query defined for its {callerMethod.Replace("StateAsync", "")} method.", innerException)
        {
        }
    }
}
