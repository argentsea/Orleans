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
    public class ArgentSeaShardPersistenceOptionsValidator<TShardOptions> : IConfigurationValidator where TShardOptions : class, IShardSetsConfigurationOptions, new()
    {
        private readonly OrleansShardPersistenceOptions options;
        private readonly string name;
        private readonly ShardSetsBase<TShardOptions>.ShardSet shardSet;

        public ArgentSeaShardPersistenceOptionsValidator(OrleansShardPersistenceOptions options, ShardSetsBase<TShardOptions>.ShardSet shardSet, string name) 
        {
            this.options = options;
            this.name = name;
            this.shardSet = shardSet;
        }

        public void ValidateConfiguration()
        {
            // Options
            if (this.options is null)
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaShardGrainPersistence<TShardOptions>)}. {nameof(OrleansShardPersistenceOptions)} must be defined.");
            }
            if (String.IsNullOrEmpty(this.options.ShardSetKey))
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaShardGrainPersistence<TShardOptions>)}. {nameof(OrleansShardPersistenceOptions)}.{nameof(OrleansShardPersistenceOptions.ShardSetKey)} must be defined.");
            }
            if (this.options.ShardSetKey != this.name)
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaShardGrainPersistence<TShardOptions>)}. {nameof(OrleansShardPersistenceOptions)}.{nameof(OrleansShardPersistenceOptions.ShardSetKey)} must be “{ this.name }”.");
            }
            if (this.options.Queries is null)
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaShardGrainPersistence<TShardOptions>)}. {nameof(OrleansShardPersistenceOptions)}.{nameof(OrleansShardPersistenceOptions.Queries)} cannot be null.");
            }
            if (this.options.Queries.Count == 0)
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaShardGrainPersistence<TShardOptions>)}. {nameof(OrleansShardPersistenceOptions)}.{nameof(OrleansShardPersistenceOptions.Queries)} cannot be empty.");
            }

            // ShardSet
            if (this.shardSet is null)
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaShardGrainPersistence<TShardOptions>)}. An instance of {nameof(ShardSetsBase<TShardOptions>.ShardSet)} must exist in the shardset service dictionary with a key of “{this.name}”.");
            }
            if (this.shardSet.Count == 0)
            {
                throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaShardGrainPersistence<TShardOptions>)}. {nameof(ShardSetsBase<TShardOptions>.ShardSet)}[{name}] cannot have no defined shards.");
            }

            for (short i = 0; i < this.shardSet.Count; i++) 
            {
                if (string.IsNullOrEmpty(this.shardSet[i].Read?.ConnectionString))
                {
                    throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaShardGrainPersistence<TShardOptions>)}. {nameof(ShardSetsBase<TShardOptions>.ShardSet)}[{name}], connection {i} has no Read connection information defined.");
                }
                if (string.IsNullOrEmpty(this.shardSet[i].Write?.ConnectionString))
                {
                    throw new OrleansConfigurationException($"Invalid configuration of {this.name ?? nameof(ArgentSeaShardGrainPersistence<TShardOptions>)}. {nameof(ShardSetsBase<TShardOptions>.ShardSet)}[{name}], connection {i} has no Write connection information defined.");
                }
            }
        }
    }
}
