using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ArgentSea.Orleans
{
    /// <summary>
    /// When the storage provider’s Read method is invoked, but the database returns no result, this activator method is called.
    /// If a grain does not implement this interface, an empty data result will cause teh system to create a default instance and the system will attempt to set the key values.
    /// Where a more complex activation is required, such as a constructor or required values calculated, you can do this explicity with this static factory method.
    /// </summary>
    /// <typeparam name="TModel">The instance type.</typeparam>
    public interface IActivator<TModel>
    {
        /// <summary>
        /// Classes that implement this interface must provide a static method which will be called when a grain of this type needs to be activated.
        /// </summary>
        /// <typeparam name="Tmodel">The type of object to be created.</typeparam>
        /// <param name="grainType">The Orleans grain type value.</param>
        /// <param name="key">The key provided to the Read method.</param>
        /// <returns>An instance of the specified type.</returns>
        static abstract TModel Activatate<Tmodel>(string grainType, ReadOnlyMemory<byte> key);
    }
}
