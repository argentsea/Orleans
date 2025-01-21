using Microsoft.CodeAnalysis.Emit;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace ArgentSea.Orleans
{
    internal static class LoggingExtensions
    {
        public enum EventIdentifier
        {
            ExpressionTreeCreation = 1,
            MapperCacheStatus = 4,
            MapperProcessTrace = 5,
            CmdExecuted = 8,
            PersistanceStart = 13,
            PersistanceStop = 14

        }

        private static readonly Action<ILogger, string, Exception?> _persistanceLifecycleStart;
        private static readonly Action<ILogger, string, Exception?> _persistanceLifecycleStop;
        private static readonly Action<ILogger, string, Exception?> _orleansDbCacheMiss;
        private static readonly Action<ILogger, string, Exception?> _orleansDbCacheHit;
        private static readonly Action<ILogger, string, Exception?> _orleansShardCacheMiss;
        private static readonly Action<ILogger, string, Exception?> _orleansShardCacheHit;
        private static readonly Action<ILogger, string, string, Exception?> _dbExpressionTreeCreation;
        private static readonly Action<ILogger, string, string, Exception?> _shardExpressionTreeCreation;
        private static readonly Action<ILogger, string, string, Exception?> _dbExpressionTreeForActivation;
        private static readonly Action<ILogger, string, long, Exception?> _dbReadCmdExecutedTrace;
        private static readonly Action<ILogger, string, long, Exception?> _dbWriteCmdExecutedTrace;
        private static readonly Action<ILogger, string, long, Exception?> _dbClearCmdExecutedTrace;
        private static readonly Action<ILogger, short, string, long, Exception?> _shardReadCmdExecutedTrace;
        private static readonly Action<ILogger, short, string, long, Exception?> _shardWriteCmdExecutedTrace;
        private static readonly Action<ILogger, short, string, long, Exception?> _shardClearCmdExecutedTrace;
        private static readonly Action<ILogger, string, Exception?> _sqlMapperPersistanceTrace;


        static LoggingExtensions()
        {
            _persistanceLifecycleStart = LoggerMessage.Define<string>(LogLevel.Debug, new EventId((int)EventIdentifier.PersistanceStart, nameof(PersistanceLifecycleOnStart)), "Grain persistance {name} is initializing.");
            _persistanceLifecycleStop = LoggerMessage.Define<string>(LogLevel.Debug, new EventId((int)EventIdentifier.PersistanceStop, nameof(PersistanceLifecycleOnStop)), "Grain persistance {name} is terminating.");
            _orleansDbCacheMiss = LoggerMessage.Define<string>(LogLevel.Debug, new EventId((int)EventIdentifier.MapperCacheStatus, nameof(OrleansDbCacheMiss)), "No cached delegate for handling orleans database keys was initialized for type {grainType}; this is normal for the first execution.");
            _orleansDbCacheHit = LoggerMessage.Define<string>(LogLevel.Trace, new EventId((int)EventIdentifier.MapperCacheStatus, nameof(OrleansDbCacheHit)), "The cached delegate for handling orleans database keys was already initialized for type {grainType}.");
            _orleansShardCacheMiss = LoggerMessage.Define<string>(LogLevel.Debug, new EventId((int)EventIdentifier.MapperCacheStatus, nameof(OrleansShardCacheMiss)), "No cached delegate for handling orleans sharded keys was initialized for type {grainType}; this is normal for the first execution.");
            _orleansShardCacheHit = LoggerMessage.Define<string>(LogLevel.Trace, new EventId((int)EventIdentifier.MapperCacheStatus, nameof(OrleansShardCacheHit)), "The cached delegate for handling orleans sharded keys was already initialized for type {grainType}.");
            _dbExpressionTreeCreation = LoggerMessage.Define<string, string>(LogLevel.Debug, new EventId((int)EventIdentifier.ExpressionTreeCreation, nameof(CreatedExpressionTreeForDatabaseKey)), "Compiled code to expression tree for grain {grainType} key mapping for database as:\r\n{code}.");
            _shardExpressionTreeCreation = LoggerMessage.Define<string, string>(LogLevel.Debug, new EventId((int)EventIdentifier.ExpressionTreeCreation, nameof(CreatedExpressionTreeForShardKey)), "Compiled code to expression tree for grain {grainType} key mapping for shards as:\r\n{code}.");
            _dbExpressionTreeForActivation = LoggerMessage.Define<string, string>(LogLevel.Debug, new EventId((int)EventIdentifier.ExpressionTreeCreation, nameof(CreatedExpressionTreeForDatabaseKey)), "Compiled code to expression tree for grain {grainType} key mapping for database as:\r\n{code}.");
            _dbReadCmdExecutedTrace = LoggerMessage.Define<string, long>(LogLevel.Trace, new EventId((int)EventIdentifier.CmdExecuted, nameof(TraceDbReadCmdExecuted)), "Executed read Db command on grainType {grainType} in {milliseconds} milliseconds.");
            _dbWriteCmdExecutedTrace = LoggerMessage.Define<string, long>(LogLevel.Trace, new EventId((int)EventIdentifier.CmdExecuted, nameof(TraceDbWriteCmdExecuted)), "Executed write Db command on grainType {grainType} in {milliseconds} milliseconds.");
            _dbClearCmdExecutedTrace = LoggerMessage.Define<string, long>(LogLevel.Trace, new EventId((int)EventIdentifier.CmdExecuted, nameof(TraceDbClearCmdExecuted)), "Executed clear Db command on grainType {grainType} in {milliseconds} milliseconds.");
            _shardReadCmdExecutedTrace = LoggerMessage.Define<short, string, long>(LogLevel.Trace, new EventId((int)EventIdentifier.CmdExecuted, nameof(TraceShardReadCmdExecuted)), "Executed read command on shard {shardId} for grainType {grainType} in {milliseconds} milliseconds.");
            _shardWriteCmdExecutedTrace = LoggerMessage.Define<short, string, long>(LogLevel.Trace, new EventId((int)EventIdentifier.CmdExecuted, nameof(TraceShardWriteCmdExecuted)), "Executed write command on shard {shardId} for grainType {grainType} in {milliseconds} milliseconds.");
            _shardClearCmdExecutedTrace = LoggerMessage.Define<short, string, long>(LogLevel.Trace, new EventId((int)EventIdentifier.CmdExecuted, nameof(TraceShardClearCmdExecuted)), "Executed clear command on shard {shardId} for grainType {grainType} in {milliseconds} milliseconds.");
            _sqlMapperPersistanceTrace = LoggerMessage.Define<string>(LogLevel.Trace, new EventId((int)EventIdentifier.MapperProcessTrace, nameof(TraceMapperGrainPersistance)), "Grain persistance mapper is now processing property {name}.");
        }

        public static void PersistanceLifecycleOnStart(this ILogger logger, string name)
            => _persistanceLifecycleStart(logger, name, null);

        public static void PersistanceLifecycleOnStop(this ILogger logger, string name)
            => _persistanceLifecycleStop(logger, name, null);

        public static void OrleansDbCacheHit(this ILogger logger, string grainType)
            => _orleansDbCacheHit(logger, grainType, null);

        public static void OrleansDbCacheMiss(this ILogger logger, string grainType)
            => _orleansDbCacheMiss(logger, grainType, null);

        public static void OrleansShardCacheHit(this ILogger logger, string grainType)
            => _orleansShardCacheHit(logger, grainType, null);

        public static void OrleansShardCacheMiss(this ILogger logger, string grainType)
            => _orleansShardCacheMiss(logger, grainType, null);

        public static void TraceMapperGrainPersistance(this ILogger logger, string propertyName)
            => _sqlMapperPersistanceTrace(logger, propertyName, null);

        public static void CreatedExpressionTreeForDatabaseKey(this ILogger logger, string grainType, Expression codeBlock)
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                using (System.IO.StringWriter writer = new System.IO.StringWriter(CultureInfo.CurrentCulture))
                {
                    DebugViewWriter.WriteTo(codeBlock, writer);
                    _dbExpressionTreeCreation(logger, grainType, writer.ToString(), null);
                }
            }
        }
        public static void CreatedExpressionTreeForShardKey(this ILogger logger, string grainType, Expression codeBlock)
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                using (System.IO.StringWriter writer = new System.IO.StringWriter(CultureInfo.CurrentCulture))
                {
                    DebugViewWriter.WriteTo(codeBlock, writer);
                    _shardExpressionTreeCreation(logger, grainType, writer.ToString(), null);
                }
            }
        }
        public static void CreatedExpressionTreeForActivation(this ILogger logger, string grainType, Expression codeBlock)
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                using (System.IO.StringWriter writer = new System.IO.StringWriter(CultureInfo.CurrentCulture))
                {
                    DebugViewWriter.WriteTo(codeBlock, writer);
                    _dbExpressionTreeForActivation(logger, grainType, writer.ToString(), null);
                }
            }
        }

        public static void TraceDbReadCmdExecuted(this ILogger logger, string grainType, long milliseconds)
        {
            if (logger.IsEnabled(LogLevel.Trace))
            {
                _dbReadCmdExecutedTrace(logger, grainType, milliseconds, null);
            }
        }
        public static void TraceDbWriteCmdExecuted(this ILogger logger, string grainType, long milliseconds)
        {
            if (logger.IsEnabled(LogLevel.Trace))
            {
                _dbWriteCmdExecutedTrace(logger, grainType, milliseconds, null);
            }
        }
        public static void TraceDbClearCmdExecuted(this ILogger logger, string grainType, long milliseconds)
        {
            if (logger.IsEnabled(LogLevel.Trace))
            {
                _dbClearCmdExecutedTrace(logger, grainType, milliseconds, null);
            }
        }
        public static void TraceShardReadCmdExecuted(this ILogger logger, short shardId, string grainType, long milliseconds)
        {
            if (logger.IsEnabled(LogLevel.Trace))
            {
                _shardReadCmdExecutedTrace(logger, shardId, grainType, milliseconds, null);
            }
        }
        public static void TraceShardWriteCmdExecuted(this ILogger logger, short shardId, string grainType, long milliseconds)
        {
            if (logger.IsEnabled(LogLevel.Trace))
            {
                _shardWriteCmdExecutedTrace(logger, shardId, grainType, milliseconds, null);
            }
        }
        public static void TraceShardClearCmdExecuted(this ILogger logger, short shardId, string grainType, long milliseconds)
        {
            if (logger.IsEnabled(LogLevel.Trace))
            {
                _shardClearCmdExecutedTrace(logger, shardId, grainType, milliseconds, null);
            }
        }
    }
}
