// © John Hicks. All rights reserved. Licensed under the MIT license.
// See the LICENSE file in the repository root for more information.

using Microsoft.Extensions.Options;
using Orleans.Serialization;
using Orleans.Storage;

namespace ArgentSea.Orleans
{


    public class OrleansDbPersistenceOptions : IStorageProviderSerializerOptions
    {
        public OrleansDbPersistenceOptions()
        { }

        public OrleansDbPersistenceOptions(string databaseKey, IList<OrleansDbQueryDefinitions> definitions)
        {
            this.DatabaseKey = databaseKey;
            this.Queries = new Dictionary<string, OrleansDbQueryDefinitions>(definitions.ToDictionary(d => d.GrainType));
        }

        /// <summary>
        /// This is both the database key in the configuration file and also the Orleans provider key.
        /// </summary>
        public string DatabaseKey { get; set; } = "Common";

        /// <summary>
        /// This is a dictionary of available CRUD queries for each grain type.
        /// </summary>
        public Dictionary<string, OrleansDbQueryDefinitions> Queries = [];


        /// <summary>
        /// This is not used but is required by the Orleans interface.
        /// </summary>
        IGrainStorageSerializer IStorageProviderSerializerOptions.GrainStorageSerializer { get; set; } =
            new JsonGrainStorageSerializer(
                new OrleansJsonSerializer(
                    Options.Create(new OrleansJsonSerializerOptions())
                )
            );


        /// <summary>
        /// Stage of silo lifecycle where storage should be initialized. Storage must be initialized prior to use.
        /// </summary>
        public int InitStage { get; set; } = ServiceLifecycleStage.ApplicationServices;

    }
}
