// © John Hicks. All rights reserved. Licensed under the MIT license.
// See the LICENSE file in the repository root for more information.

using Microsoft.Extensions.Options;
using Orleans.Providers;
using Orleans.Serialization;
using Orleans.Storage;

namespace ArgentSea.Orleans
{


    public class OrleansShardPersistenceOptions : IStorageProviderSerializerOptions
    {
        public OrleansShardPersistenceOptions()
        { }

        public OrleansShardPersistenceOptions(string shardSetKey, IList<OrleansShardQueryDefinitions> definitions)
        {
            this.ShardSetKey = shardSetKey;
            this.Queries = new Dictionary<string, OrleansShardQueryDefinitions>(definitions.ToDictionary(d => d.GrainType));
        }

        /// <summary>
        /// This is both the shard set key in the configuration file and also the Orleans provider key.
        /// </summary>
        public string ShardSetKey { get; set; } = ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME;


        /// <summary>
        /// If true, an error is thrown if the Orleans grainId and model key do not match. Otherwise, only the model key is used.
        /// </summary>
        public bool ValidateGrainKeys { get; set; } = true;

        /// <summary>
        /// This is a dictionary of available CRUD queries for each grain type.
        /// </summary>
        public Dictionary<string, OrleansShardQueryDefinitions> Queries = [];

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
        /// Stage of silo lifecycle where storage should be initialized.  Storage must be initialized prior to use.
        /// </summary>
        public int InitStage { get; set; } = ServiceLifecycleStage.ApplicationServices;
    }
}
