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
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Data.Common;
using System.Data;
using System.Diagnostics;

namespace ArgentSea.Orleans
{

    public class ArgentSeaDbGrainPersistence<TDatabaseOptions> : IGrainStorage where TDatabaseOptions : class, IDatabaseConfigurationOptions, new() //, ILifecycleParticipant<ISiloLifecycle> 
    {

        private readonly string serviceId;
        private readonly ILogger logger;
        private readonly string name;
        private readonly DatabasesBase<TDatabaseOptions>.Database database;
        private readonly Dictionary<string, OrleansDbQueryDefinitions> queryDefinitions;
        //private readonly int initStage;

        private static readonly ConcurrentDictionary<string, Lazy<Action<ReadOnlyMemory<byte>, ParameterCollection, ILogger>>> _setParameters = new();

        public ArgentSeaDbGrainPersistence(
            DatabasesBase<TDatabaseOptions> dbs, 
            IOptions<OrleansDbPersistenceOptions> orleansOptions, 
            IOptions<ClusterOptions> clusterOptions,
            ILogger logger)
        {
            var dbKey = orleansOptions.Value.DatabaseKey;
            this.name = dbKey;
            this.database = dbs[dbKey];
            this.queryDefinitions = orleansOptions.Value.Queries;
            this.serviceId = clusterOptions.Value.ServiceId;
            this.logger = logger;
            //this.initStage = orleansOptions.Value.InitStage;
        }

        //public void Participate(ISiloLifecycle observer)
        //{
        //    var name = OptionFormattingUtilities.Name<ArgentSeaDbGrainPersistence<TDatabaseOptions>>(this.name);
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

            var lazyParamSetter = _setParameters.GetOrAdd(grainType, (key) => new Lazy<Action<ReadOnlyMemory<byte>, ParameterCollection, ILogger>>(() => OrleansExpressionHelper.BuildDbReadLambda<TModel>(grainType, this.logger), LazyThreadSafetyMode.ExecutionAndPublication));
            if (lazyParamSetter.IsValueCreated)
            {
                LoggingExtensions.OrleansDbCacheHit(logger, grainType);
            }
            else
            {
                LoggingExtensions.OrleansDbCacheMiss(logger, grainType);
            }

            lazyParamSetter.Value(grainId.Key.Value, prms, this.logger);

            if (grainState.State is null)
            {
                grainState.State = Activator.CreateInstance<TModel>();
            }

            if (queries.ResultFormat == QueryResultFormat.ResultSet)
            {
                await this.database.Read.MapReaderAsync<TModel>(grainState.State, queries.ReadQuery, prms, CancellationToken.None);
            }
            else
            {
                await this.database.Read.MapOutputAsync<TModel>(grainState.State, queries.ReadQuery, prms, CancellationToken.None);
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
            this.logger?.TraceDbReadCmdExecuted(grainType, elapsedMS);
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

            await this.database.Write.RunAsync(queries.WriteQuery, prms, CancellationToken.None);
            var elapsedMS = (long)((Stopwatch.GetTimestamp() - startTimestamp) * TimestampToMilliseconds);
            this.logger?.TraceDbWriteCmdExecuted(grainType, elapsedMS);
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

            var lazyParamSetter = _setParameters.GetOrAdd(grainType, (key) => new Lazy<Action<ReadOnlyMemory<byte>, ParameterCollection, ILogger>>(() => OrleansExpressionHelper.BuildDbReadLambda<TModel>(grainType, this.logger), LazyThreadSafetyMode.ExecutionAndPublication));
            if (lazyParamSetter.IsValueCreated)
            {
                LoggingExtensions.OrleansDbCacheHit(logger, grainType);
            }
            else
            {
                LoggingExtensions.OrleansDbCacheMiss(logger, grainType);
            }
            lazyParamSetter.Value(grainId.Key.Value, prms, this.logger);


            await this.database.Write.RunAsync(queries.ClearQuery, prms, CancellationToken.None);
            grainState.RecordExists = false;
            var elapsedMS = (long)((Stopwatch.GetTimestamp() - startTimestamp) * TimestampToMilliseconds);
            this.logger?.TraceDbClearCmdExecuted(grainType, elapsedMS);
        }

        private static readonly double TimestampToMilliseconds = (double)TimeSpan.TicksPerSecond / (Stopwatch.Frequency * TimeSpan.TicksPerMillisecond);

        //private Task Init(CancellationToken cancellationToken)
        //{
        //    cancellationToken.ThrowIfCancellationRequested();
        //    this.logger?.PersistanceLifecycleOnStart(this.name);
        //    if (this.database is null)
        //    {
        //        throw new Exception($"The database {this.name} is not defined in the configuration.");
        //    }
        //    return Task.CompletedTask;
        //}

        //private Task Close(CancellationToken cancellationToken)
        //{
        //    cancellationToken.ThrowIfCancellationRequested();
        //    this.logger?.PersistanceLifecycleOnStop(this.name);
        //    return Task.CompletedTask;
        //}
    }
}
