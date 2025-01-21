// © John Hicks. All rights reserved. Licensed under the MIT license.
// See the LICENSE file in the repository root for more information.

using System;
using Orleans.Storage;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using ArgentSea.Orleans;
using System.Collections.Concurrent;
using Orleans;
using System.Linq.Expressions;
using System.Reflection;
using Newtonsoft.Json;
using System.Xml.Linq;
using System.Runtime.CompilerServices;
using System.Collections;
using Orleans.Runtime;
using System.Data.Common;
using System.Data;
using System.Diagnostics;

namespace ArgentSea.Orleans
{

    public class ArgentSeaShardGrainPersistence<TShardOptions> : IGrainStorage where TShardOptions : class, IShardSetsConfigurationOptions, new() //ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly string serviceId;
        private readonly ILogger? logger;
        private readonly string name;
        private readonly ShardSetsBase<TShardOptions>.ShardSet shards;
        private readonly Dictionary<string, OrleansShardQueryDefinitions> queryDefinitions;
        //private readonly int initStage;
        private readonly bool validateGrainKeys;

        private static readonly ConcurrentDictionary<string, Lazy<Func<ReadOnlyMemory<byte>, ParameterCollection, ILogger?, short>>> _readLambda = new();
        private static readonly ConcurrentDictionary<string, Lazy<Delegate>> _writeLambda = new();
        //private static readonly ConcurrentDictionary<string, Lazy<Func<ReadOnlyMemory<byte>, object, ParameterCollection, ILogger?, short>>> _clearLambda = new();
        private static readonly ConcurrentDictionary<string, Lazy<Delegate>> _clearLambda = new();
        private static readonly ConcurrentDictionary<string, Lazy<Delegate>> _setShardLambda = new();

        public ArgentSeaShardGrainPersistence(
            ShardSetsBase<TShardOptions> shards,
            IOptions<OrleansShardPersistenceOptions> orleansOptions, 
            IOptions<ClusterOptions> clusterOptions, 
            ILogger<ArgentSeaShardGrainPersistence<TShardOptions>>? logger)
        {
            var shardSetKey = orleansOptions.Value.ShardSetKey;
            this.name = shardSetKey;
            this.shards = shards[shardSetKey];
            this.queryDefinitions = orleansOptions.Value.Queries;
            this.serviceId = clusterOptions.Value.ServiceId;
            this.logger = logger;
            //this.initStage = orleansOptions.Value.InitStage;
            this.validateGrainKeys = orleansOptions.Value.ValidateGrainKeys;
        }

        //public void Participate(ISiloLifecycle observer)
        //{
        //    var name = OptionFormattingUtilities.Name<ArgentSeaShardGrainPersistence<TShardOptions>>(this.name);
        //    observer.Subscribe(name, this.initStage, Init, Close);
        //}

        public async Task ReadStateAsync<TModel>(string grainType, GrainId grainId, IGrainState<TModel> grainState)
        {
            var startTimestamp = Stopwatch.GetTimestamp();
            if (!this.queryDefinitions.TryGetValue(grainType, out var queries))
            {
                throw new OrleansQueryNotProvidedException(grainType);
            }
            var prms = new ParameterCollection();


            var lazyShardIdAndParams = _readLambda.GetOrAdd(grainType, new Lazy<Func<ReadOnlyMemory<byte>, ParameterCollection, ILogger?, short>>(() => OrleansExpressionHelper.BuildShardReadLambda<TModel>(grainType, this.logger), LazyThreadSafetyMode.ExecutionAndPublication));
            if (lazyShardIdAndParams.IsValueCreated)
            {
                logger?.OrleansShardCacheHit(grainType);
            }
            else
            {
                logger?.OrleansShardCacheMiss(grainType);
            }
            var shardId = lazyShardIdAndParams.Value(grainId.Key.Value, prms, this.logger);

            if (grainState.State is null)
            {
                if (grainState.State is IActivator<TModel>)
                {
                    var miActivate = typeof(TModel).GetMethod(nameof(IActivator<TModel>.Activatate), BindingFlags.Static | BindingFlags.Public)!;
                    grainState.State = (TModel)miActivate.Invoke(null, [grainType, grainId.Key.Value])!;
                }
                else
                {
                    grainState.State = Activator.CreateInstance<TModel>();
                    var lazySetShard = _setShardLambda.GetOrAdd(grainType, new Lazy<Delegate> (() => OrleansExpressionHelper.BuildSetShardLambda<TModel>(grainType, this.logger), LazyThreadSafetyMode.ExecutionAndPublication));
                    if (lazySetShard.IsValueCreated)
                    {
                        logger?.OrleansDbCacheHit(grainType);
                    }
                    else
                    {
                        logger?.OrleansDbCacheMiss(grainType);
                    }
                    ((Action<ReadOnlyMemory<byte>, TModel, ILogger?>)lazySetShard.Value)(grainId.Key.Value, grainState.State!, this.logger);
                }

                grainState.RecordExists = false;
            }
            else
            {
                grainState.RecordExists = true;
            }

            if (grainState.State is null)
            {
                grainState.State = Activator.CreateInstance<TModel>();
            }

            if (queries.ResultFormat == QueryResultFormat.ResultSet)
            {
                await this.shards[shardId].Read.MapReaderAsync<TModel>(grainState.State, queries.ReadQuery, prms, CancellationToken.None);
            }
            else
            {
                await this.shards[shardId].Read.MapOutputAsync<TModel>(grainState.State, queries.ReadQuery, prms, CancellationToken.None);
            }
            if (grainState.State is null)
            {
                if (grainState.State is IActivator<TModel>)
                {
                    var miActivate = typeof(TModel).GetMethod(nameof(IActivator<TModel>.Activatate), BindingFlags.Static | BindingFlags.Public)!;
                    grainState.State = (TModel)miActivate.Invoke(null, [grainType, grainId.Key.Value])!;
                }
                else
                {
                    grainState.State = Activator.CreateInstance<TModel>();
                    // todo: set properties
                }

                grainState.RecordExists = false;
            }
            else
            {
                grainState.RecordExists = true;
            }
            var elapsedMS = (long)((Stopwatch.GetTimestamp() - startTimestamp) * TimestampToMilliseconds);
            this.logger?.TraceShardReadCmdExecuted(shardId, grainType, elapsedMS);
        }


        public async Task WriteStateAsync<TModel>(string grainType, GrainId grainId, IGrainState<TModel> grainState)
        {
            var startTimestamp = Stopwatch.GetTimestamp();
            if (grainState.State is null)
            {
                throw new NoNullAllowedException($"The grain state cannot be written because it is null.");
            }
            if (!this.queryDefinitions.TryGetValue(grainType, out var queries))
            {
                throw new OrleansQueryNotProvidedException($"The grain type {grainType} is not defined in the configuration.");
            }
            var prms = new ParameterCollection()
                .CreateInputParameters<TModel>(grainState.State, this.logger);


            var lazyShardId = _writeLambda.GetOrAdd(grainType, new Lazy<Delegate>(() => OrleansExpressionHelper.BuildShardWriteLambda<TModel>(grainType, this.validateGrainKeys), LazyThreadSafetyMode.ExecutionAndPublication));
            if (lazyShardId.IsValueCreated)
            {
                logger?.OrleansShardCacheHit(grainType);
            }
            else
            {
                logger?.OrleansShardCacheMiss(grainType);
            }
            var lzyValue = (Func<ReadOnlyMemory<byte>, TModel, short>)lazyShardId.Value;
            var shardId = lzyValue(grainId.Key.Value, grainState.State);


            await this.shards[shardId].Write.RunAsync(queries.WriteQuery, prms, CancellationToken.None);
            var elapsedMS = (long)((Stopwatch.GetTimestamp() - startTimestamp) * TimestampToMilliseconds);
            this.logger?.TraceShardWriteCmdExecuted(shardId, grainType, elapsedMS);
        }

        public async Task ClearStateAsync<TModel>(string grainType, GrainId grainId, IGrainState<TModel> grainState)
        {
            var startTimestamp = Stopwatch.GetTimestamp();
            if (grainState.State is null)
            {
                throw new NoNullAllowedException($"The grain state cannot be cleared because it is null.");
            }
            if (!this.queryDefinitions.TryGetValue(grainType, out var queries))
            {
                throw new OrleansQueryNotProvidedException($"The grain type {grainType} is not defined in the configuration.");
            }
            var prms = new ParameterCollection();

            var lazyShardIdAndParams = _clearLambda.GetOrAdd(grainType, new Lazy<Delegate>(() => OrleansExpressionHelper.BuildShardClearLambda<TModel>(grainType, this.validateGrainKeys, this.logger), LazyThreadSafetyMode.ExecutionAndPublication));
            if (lazyShardIdAndParams.IsValueCreated)
            {
                logger?.OrleansShardCacheHit(grainType);
            }
            else
            {
                logger?.OrleansShardCacheMiss(grainType);
            }
            var lzy = (Func<ReadOnlyMemory<byte>, object, ParameterCollection, ILogger?, short>)lazyShardIdAndParams.Value;
            var shardId = lzy(grainId.Key.Value, grainState.State, prms, this.logger);

            await this.shards[shardId].Write.RunAsync(queries.ClearQuery, prms, CancellationToken.None);
            grainState.RecordExists = false;
            var elapsedMS = (long)((Stopwatch.GetTimestamp() - startTimestamp) * TimestampToMilliseconds);
            this.logger?.TraceDbClearCmdExecuted(grainType, elapsedMS);
        }

        private static readonly double TimestampToMilliseconds = (double)TimeSpan.TicksPerSecond / (Stopwatch.Frequency * TimeSpan.TicksPerMillisecond);

        //private Task Init (CancellationToken cancellationToken)
        //{
        //    this.logger?.PersistanceLifecycleOnStart(this.name);
        //    if (this.shards is null || this.shards.Count == 0)
        //    {
        //        throw new Exception($"The ShardSet {this.name} is not defined in the configuration.");
        //    }
        //    return Task.CompletedTask;
        //}

        //private Task Close(CancellationToken cancellationToken)
        //{
        //    this.logger?.PersistanceLifecycleOnStop(this.name);
        //    return Task.CompletedTask;
        //}
    }
}
