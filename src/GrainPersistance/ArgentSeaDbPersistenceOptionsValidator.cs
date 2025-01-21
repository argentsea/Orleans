// © John Hicks. All rights reserved. Licensed under the MIT license.
// See the LICENSE file in the repository root for more information.

using ArgentSea;
using Orleans.Storage;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace ArgentSea.Orleans
{
    internal class ArgentSeaDbPersistenceOptionsValidator<TDatabaseOptions> : IConfigurationValidator where TDatabaseOptions : class, IDatabaseConfigurationOptions, new()
    {
        private readonly OrleansDbPersistenceOptions options;
        private readonly string name;
        private readonly DatabasesBase<TDatabaseOptions>.Database database;

        public ArgentSeaDbPersistenceOptionsValidator(string name, OrleansDbPersistenceOptions options, DatabasesBase<TDatabaseOptions>.Database database) 
        {
            this.options = options;
            this.name = name;
            this.database = database;
        }

        public void ValidateConfiguration()
        {
            // Options
            if (this.options is null)
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaDbGrainPersistence<TDatabaseOptions>)}. {nameof(OrleansDbPersistenceOptions)} cannot be null.");
            }
            if (String.IsNullOrEmpty(this.options.DatabaseKey))
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaDbGrainPersistence<TDatabaseOptions>)}. {nameof(OrleansDbPersistenceOptions)}.{nameof(OrleansDbPersistenceOptions.DatabaseKey)} cannot be null.");
            }
            if (this.options.DatabaseKey != this.name)
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaDbGrainPersistence<TDatabaseOptions>)}. {nameof(OrleansDbPersistenceOptions)}.{nameof(OrleansDbPersistenceOptions.DatabaseKey)} must be “{this.name}”.");
            }
            if (this.options.Queries is null)
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaDbGrainPersistence<TDatabaseOptions>)}. {nameof(OrleansDbPersistenceOptions)}.{nameof(OrleansDbPersistenceOptions.Queries)} cannot be null.");
            }
            if (this.options.Queries.Count == 0)
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaDbGrainPersistence<TDatabaseOptions>)}. {nameof(OrleansDbPersistenceOptions)}.{nameof(OrleansDbPersistenceOptions.Queries)} cannot be empty.");
            }


            // Database
            if (this.database is null)
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaDbGrainPersistence<TDatabaseOptions>)}. An instance of a {nameof(DatabasesBase<TDatabaseOptions>.Database)} must exist in the database service dictionary with a key of “{this.name }”.");
            }
            if (string.IsNullOrEmpty(this.database.Read?.ConnectionString))
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaDbGrainPersistence<TDatabaseOptions>)}. {nameof(DatabasesBase<TDatabaseOptions>.Database)} has no Read connection information defined.");
            }
            if (string.IsNullOrEmpty(this.database.Write?.ConnectionString))
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaDbGrainPersistence<TDatabaseOptions>)}. {nameof(DatabasesBase<TDatabaseOptions>.Database)} has no Write connection information defined.");
            }
        }
    }
}
