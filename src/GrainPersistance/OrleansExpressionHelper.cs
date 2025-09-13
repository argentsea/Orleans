using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ArgentSea.Orleans
{
    internal static class OrleansExpressionHelper
    {
        // parses the grainId value to return the shardId and sets the query parameters embedded in the grainId.
        internal static Func<ReadOnlyMemory<byte>, ParameterCollection, ILogger?, short> BuildShardReadLambda<TModel>(string grainType, ILogger? logger)
        {
            var tModel = typeof(TModel);
            ParameterExpression expROMemory = Expression.Parameter(typeof(ReadOnlyMemory<byte>), "rom");
            ParameterExpression expPrms = Expression.Parameter(typeof(ParameterCollection), "prms");
            ParameterExpression expLogger = Expression.Parameter(typeof(ILogger), "logger");
            ParameterExpression expIgnoreParameters = Expression.Variable(typeof(HashSet<string>), "ignorePrms");
            List<ParameterExpression> variables = new() { expIgnoreParameters };
            List<Expression> expressions = new();
            var exprInPrms = new ParameterExpression[] { expROMemory, expPrms, expLogger };

            var miGetString = typeof(UTF8Encoding).GetMethod(nameof(UTF8Encoding.GetString), [typeof(ReadOnlySpan<byte>)]);
            var miLogTrace = typeof(LoggingExtensions).GetMethod(nameof(LoggingExtensions.TraceMapperGrainPersistance));
            var miSpan = typeof(ReadOnlyMemory<byte>).GetProperty(nameof(ReadOnlyMemory<byte>.Span), BindingFlags.Public | BindingFlags.Instance)!.GetGetMethod()!;

            // Check for ShardKey marked IsRecordIdentifier, return first instance.
            foreach (var prop in tModel.GetProperties())
            {
                Type propType = prop.PropertyType;
                var shdAttr = ExpressionHelpers.GetMapShardKeyAttribute(prop, propType, out var isNullable, out var isShardKey, out var isShardChild, out var isShardGrandChild, out var isShardGreatGrandChild);
                if (shdAttr is not null && shdAttr.IsRecordIdentifier && !isNullable && (isShardKey || isShardChild || isShardGrandChild || isShardGrandChild))
                {
                    var expToSpan = Expression.Call(expROMemory, miSpan);
                    //var expShardCall = Expression.Call(propType.GetMethod(nameof(ShardKey<int>.FromUtf8), BindingFlags.Static | BindingFlags.Public)!, expToSpan);
                    var expShardNew = Expression.New(propType.GetConstructor( [ typeof(ReadOnlySpan<byte>)] )!, [expToSpan]);
                    var expShardKey = Expression.Variable(propType, "shardKey");
                    variables.Add(expShardKey);
                    expressions.Add(Expression.Assign(expShardKey, expShardNew));
                    var miGetShardId = propType.GetProperty(nameof(ShardKey<int>.ShardId), BindingFlags.Public | BindingFlags.Instance)!.GetGetMethod();
                    var expShardId = Expression.Call(expShardKey, miGetShardId!);

                    expressions.Add(Expression.Assign(expIgnoreParameters, Expression.New(typeof(HashSet<string>))));

                    var foundPrms = false;
                    var found = ExpressionHelpers.ShardKeyInMapProperties(prop, propType, shdAttr, isNullable, isShardKey, isShardChild, isShardGrandChild, isShardGreatGrandChild, expShardKey, expressions, variables, expPrms, expIgnoreParameters, expLogger, new HashSet<string>(), miLogTrace, ref foundPrms, logger);
                    expressions.Add(expShardId); // return the shardId


                    var inBlock = Expression.Block(variables, expressions);
                    var lmbIn = Expression.Lambda<Func<ReadOnlyMemory<byte>, ParameterCollection, ILogger?, short>>(inBlock, exprInPrms);
                    logger?.CreatedExpressionTreeForShardKey(grainType, inBlock);
                    return lmbIn.Compile();
                }
            }
            // No explicit key set, now check for non-nullable ShardKey, return first instance.
            foreach (var prop in tModel.GetProperties())
            {
                // identitcal to if block above, except for absense of IsRecordIdentifier check
                Type propType = prop.PropertyType;
                var shdAttr = ExpressionHelpers.GetMapShardKeyAttribute(prop, propType, out var isNullable, out var isShardKey, out var isShardChild, out var isShardGrandChild, out var isShardGreatGrandChild);
                if (shdAttr is not null && !isNullable && (isShardKey || isShardChild || isShardGrandChild || isShardGrandChild))
                {
                    var expToSpan = Expression.Call(expROMemory, miSpan);
                    //var expShardCall = Expression.Call(propType.GetMethod(nameof(ShardKey<int>.FromUtf8), BindingFlags.Static | BindingFlags.Public)!, expToSpan);
                    var expShardNew = Expression.New(propType.GetConstructor([typeof(ReadOnlySpan<byte>)])!, [expToSpan]);
                    var expShardKey = Expression.Variable(propType, "shardKey");
                    variables.Add(expShardKey);

                    expressions.Add(Expression.Assign(expShardKey, expShardNew));
                    var miGetShardId = propType.GetProperty(nameof(ShardKey<int>.ShardId), BindingFlags.Public | BindingFlags.Instance)!.GetGetMethod();
                    var expShardId = Expression.Call(expShardKey, miGetShardId!);

                    expressions.Add(Expression.Assign(expIgnoreParameters, Expression.New(typeof(HashSet<string>))));

                    var foundPrms = false;
                    var found = ExpressionHelpers.ShardKeyInMapProperties(prop, propType, shdAttr, isNullable, isShardKey, isShardChild, isShardGrandChild, isShardGreatGrandChild, expShardKey, expressions, variables, expPrms, expIgnoreParameters, expLogger, new HashSet<string>(), miLogTrace, ref foundPrms, logger);
                    expressions.Add(expShardId); // return the shardId


                    var inBlock = Expression.Block(variables, expressions);
                    var lmbIn = Expression.Lambda<Func<ReadOnlyMemory<byte>, ParameterCollection, ILogger?, short>>(inBlock, exprInPrms);
                    logger?.CreatedExpressionTreeForShardKey(grainType, inBlock);
                    return lmbIn.Compile();
                }
            }
            throw new ShardKeyNotFoundException(grainType);
        }

        // parses the grainId value to return the shardId and sets the query parameters embedded in the grainId.
        internal static Action<ReadOnlyMemory<byte>, TModel, ILogger?> BuildSetShardLambda<TModel>(string grainType, ILogger? logger)
        {
            var tModel = typeof(TModel);
            ParameterExpression expROMemory = Expression.Parameter(typeof(ReadOnlyMemory<byte>), "rom");
            ParameterExpression expModel = Expression.Parameter(typeof(TModel), "model");
            ParameterExpression expLogger = Expression.Parameter(typeof(ILogger), "logger");
            List<Expression> expressions = new();
            var exprInPrms = new ParameterExpression[] { expROMemory, expModel, expLogger };

            var miLogTrace = typeof(LoggingExtensions).GetMethod(nameof(LoggingExtensions.TraceMapperGrainPersistance));
            var miSpan = typeof(ReadOnlyMemory<byte>).GetProperty(nameof(ReadOnlyMemory<byte>.Span), BindingFlags.Public | BindingFlags.Instance)!.GetGetMethod()!;

            // Check for ShardKey marked IsRecordIdentifier, return first instance.
            foreach (var prop in tModel.GetProperties())
            {
                Type propType = prop.PropertyType;
                var shdAttr = ExpressionHelpers.GetMapShardKeyAttribute(prop, propType, out var isNullable, out var isShardKey, out var isShardChild, out var isShardGrandChild, out var isShardGreatGrandChild);
                if (shdAttr is not null && shdAttr.IsRecordIdentifier && !isNullable && (isShardKey || isShardChild || isShardGrandChild || isShardGrandChild))
                {
                    var expProp = Expression.Property(expModel, prop);
                    var expNewShardKey = Expression.New(propType.GetConstructor([typeof(ReadOnlySpan<byte>)])!, [Expression.Call(expROMemory, miSpan)]);
                    expressions.Add(Expression.Assign(expProp, expNewShardKey));
                    var block = Expression.Block(expressions);
                    var lmbKey = Expression.Lambda<Action<ReadOnlyMemory<byte>, TModel, ILogger?>>(block, exprInPrms);
                    logger?.CreatedExpressionTreeForShardKey(grainType, block);
                    return lmbKey.Compile();
                }
            }
            // No explicit key set, now check for non-nullable ShardKey, return first instance.
            foreach (var prop in tModel.GetProperties())
            {
                // identitcal to if block above, except for absense of IsRecordIdentifier check
                Type propType = prop.PropertyType;
                var shdAttr = ExpressionHelpers.GetMapShardKeyAttribute(prop, propType, out var isNullable, out var isShardKey, out var isShardChild, out var isShardGrandChild, out var isShardGreatGrandChild);
                if (shdAttr is not null && !isNullable && (isShardKey || isShardChild || isShardGrandChild || isShardGrandChild))
                {
                    var expProp = Expression.Property(expModel, prop);
                    var expNewShardKey = Expression.New(propType.GetConstructor([typeof(ReadOnlySpan<byte>)])!, [Expression.Call(expROMemory, miSpan)]);
                    expressions.Add(Expression.Assign(expProp, expNewShardKey));
                    var block = Expression.Block(expressions);
                    var lmbKey = Expression.Lambda<Action<ReadOnlyMemory<byte>, TModel, ILogger?>>(block, exprInPrms);
                    logger?.CreatedExpressionTreeForShardKey(grainType, block);
                    return lmbKey.Compile();
                }
            }
            throw new ShardKeyNotFoundException(grainType);
        }

        // Iterates through the properties of the model (deterministic) and builds a set of data parameters corresponding to the properties that are marked with IsRecordIdentifier.
        internal static Action<ReadOnlyMemory<byte>, ParameterCollection, ILogger?> BuildDbReadLambda<TModel>(string grainType, ILogger? logger)
        {
            var tModel = typeof(TModel);
            var expressions = new List<Expression>();
            ParameterExpression expPrmSqlPrms = Expression.Variable(typeof(DbParameterCollection), "parameters");
            ParameterExpression expROM = Expression.Parameter(typeof(ReadOnlyMemory<byte>), "rom");
            ParameterExpression expLogger = Expression.Parameter(typeof(ILogger), "logger");
            var exprPrms = new ParameterExpression[] { expROM, expPrmSqlPrms, expLogger };
            var expSpan = Expression.Variable(typeof(ReadOnlySpan<byte>), "span");
            ParameterExpression expIgnoreParameters = Expression.Parameter(typeof(HashSet<string>), "ignoreParameters");
            var variables = new List<ParameterExpression>() { expSpan, expIgnoreParameters };
            var miSpan = typeof(ReadOnlyMemory<byte>).GetProperty(nameof(ReadOnlyMemory<byte>.Span), BindingFlags.Public | BindingFlags.Instance)!.GetGetMethod()!;
            expressions.Add(Expression.Assign(expSpan, Expression.Call(expROM, miSpan)));
            expressions.Add(Expression.Assign(expIgnoreParameters, Expression.New(typeof(HashSet<string>))));

            var props = tModel.GetProperties().Where(p => p.IsDefined(typeof(ParameterMapAttributeBase), true));
            foreach (var prop in props)
            {
                bool alreadyFound = false;
                var attrPMs = prop.GetCustomAttributes<ParameterMapAttributeBase>(true);
                foreach (var attrPM in attrPMs)
                {
                    if (alreadyFound)
                    {
                        throw new MultipleMapAttributesException(prop);
                    }
                    alreadyFound = true;

                    if (attrPM.IsRecordIdentifier)
                    {
                        var tProperty = prop.PropertyType;
                        if (!attrPM.IsValidType(tProperty))
                        {
                            throw new InvalidMapTypeException(prop, attrPM.SqlType, attrPM.SqlTypeName);
                        }
                        //span = span.Extract(out var key);
                        var expValue = Expression.Variable(tProperty, $"v{prop.Name}");
                        variables.Add(expValue);
                        var miCall = typeof(SpanParser).GetMethod(nameof(SpanParser.Extract), BindingFlags.Static | BindingFlags.Public, [ typeof(ReadOnlySpan<byte>), tProperty.MakeByRefType() ]);
                        if (miCall is null)
                        {
                            throw new InvalidKeyTypeException(grainType, prop.Name);
                        }
                        var expCall = Expression.Call(miCall, expSpan, expValue);
                        expressions.Add(Expression.Assign(expSpan, expCall));
                        attrPM.AppendInParameterExpressions(expressions, expPrmSqlPrms, expIgnoreParameters, new HashSet<string>(), expValue, tProperty, expLogger, logger);
                    }
                }
            }
            var expBlock = Expression.Block(variables, expressions);
            var lmbOut = Expression.Lambda<Action<ReadOnlyMemory<byte>, ParameterCollection, ILogger?>>(expBlock, exprPrms);
            logger?.CreatedExpressionTreeForDatabaseKey(grainType, expBlock);
            return lmbOut.Compile();
        }


        // retrieves the shardId from the model ShardKey, optionally compares ShardKey with grainId.
        internal static Func<ReadOnlyMemory<byte>, TModel, short> BuildShardWriteLambda<TModel>(string grainType, bool validateKey)
        {
            var tModel = typeof(TModel);
            ParameterExpression expROMemory = Expression.Parameter(typeof(ReadOnlyMemory<byte>), "rom");
            ParameterExpression expModel = Expression.Parameter(typeof(TModel), "model");
            var miGetString = typeof(UTF8Encoding).GetMethod(nameof(UTF8Encoding.GetString), [typeof(ReadOnlySpan<byte>)]);
            List<Expression> expressions = [];
            var miSpan = typeof(ReadOnlyMemory<byte>).GetProperty(nameof(ReadOnlyMemory<byte>.Span), BindingFlags.Public | BindingFlags.Instance)!.GetGetMethod()!;

            // Check for ShardKey marked IsRecordIdentifier, return first instance.
            foreach (var prop in tModel.GetProperties())
            {
                Type propType = prop.PropertyType;
                var shdAttr = ExpressionHelpers.GetMapShardKeyAttribute(prop, propType, out var isNullable, out var isShardKey, out var isShardChild, out var isShardGrandChild, out var isShardGreatGrandChild);
                if (shdAttr is not null && shdAttr.IsRecordIdentifier && !isNullable && (isShardKey || isShardChild || isShardGrandChild || isShardGrandChild))
                {
                    var expShardKey = Expression.Property(expModel, typeof(TModel).GetProperty(prop.Name)!.GetGetMethod()!);
                    if (validateKey)
                    {
                        var expToSpan = Expression.Call(expROMemory, miSpan);
                        //var expGrainIdShard = Expression.Call(propType.GetMethod(nameof(ShardKey<int>.FromUtf8), BindingFlags.Static | BindingFlags.Public)!, expToSpan);
                        var expShardNew = Expression.New(propType.GetConstructor([typeof(ReadOnlySpan<byte>)])!, [expToSpan]);
                        var expValid = Expression.IfThen(Expression.NotEqual(expShardKey, expShardNew),
                            Expression.Throw(Expression.New(typeof(OrleansIdKeyMismatchException).GetConstructor([typeof(string)])!, [Expression.Constant(grainType)])));
                        expressions.Add(expValid);
                    }

                    var miGetShardId = propType.GetProperty(nameof(ShardKey<int>.ShardId), BindingFlags.Public | BindingFlags.Instance)!.GetGetMethod();
                    expressions.Add(Expression.Call(expShardKey, miGetShardId!));
                    var inBlock = Expression.Block(expressions);

                    var lmbShardId = Expression.Lambda<Func<ReadOnlyMemory<byte>, TModel, short>>(inBlock, [expROMemory, expModel]);
                    return lmbShardId.Compile();
                }
            }
            // No explicit key set, now check for non-nullable ShardKey, return first instance.
            foreach (var prop in tModel.GetProperties())
            {
                Type propType = prop.PropertyType;
                var shdAttr = ExpressionHelpers.GetMapShardKeyAttribute(prop, propType, out var isNullable, out var isShardKey, out var isShardChild, out var isShardGrandChild, out var isShardGreatGrandChild);
                if (shdAttr is not null && shdAttr.IsRecordIdentifier && !isNullable && (isShardKey || isShardChild || isShardGrandChild || isShardGrandChild))
                {
                    var expShardKey = Expression.Property(expModel, typeof(TModel).GetProperty(prop.Name)!.GetGetMethod()!);
                    if (validateKey)
                    {
                        var expToSpan = Expression.Call(expROMemory, miSpan);
                        //var expGrainIdShard = Expression.Call(propType.GetMethod(nameof(ShardKey<int>.FromUtf8), BindingFlags.Static | BindingFlags.Public)!, expToSpan);
                        var expShardNew = Expression.New(propType.GetConstructor([typeof(ReadOnlySpan<byte>)])!, [expToSpan]);
                        var expValid = Expression.IfThen(Expression.NotEqual(expShardKey, expShardNew),
                            Expression.Throw(Expression.New(typeof(OrleansIdKeyMismatchException).GetConstructor([typeof(string)])!, [Expression.Constant(grainType)])));
                        expressions.Add(expValid);
                    }

                    var miGetShardId = propType.GetProperty(nameof(ShardKey<int>.ShardId), BindingFlags.Public | BindingFlags.Instance)!.GetGetMethod();
                    expressions.Add(Expression.Call(expShardKey, miGetShardId!));
                    var inBlock = Expression.Block(expressions);

                    var lmbShardId = Expression.Lambda<Func<ReadOnlyMemory<byte>, TModel, short>>(inBlock, [expROMemory, expModel]);
                    return lmbShardId.Compile();
                }
            }
            throw new ShardKeyNotFoundException(grainType);
        }

        // retrieves the shardId from the model ShardKey, sets the query parameters from the model values, optionally compares ShardKey with grainId.
        internal static Func<ReadOnlyMemory<byte>, TModel, ParameterCollection, ILogger?, short> BuildShardClearLambda<TModel>(string grainType, bool validateKey, ILogger? logger)
        {
            var tModel = typeof(TModel);
            ParameterExpression expROMemory = Expression.Parameter(typeof(ReadOnlyMemory<byte>), "rom");
            ParameterExpression expModel = Expression.Parameter(typeof(TModel), "model");
            ParameterExpression expPrms = Expression.Parameter(typeof(ParameterCollection), "prms");
            ParameterExpression expLogger = Expression.Parameter(typeof(ILogger), "logger");
            
            ParameterExpression expIgnoreParameters = Expression.Variable(typeof(HashSet<string>), "ignorePrms");
            List<ParameterExpression> variables = new() { expIgnoreParameters };
            List<Expression> expressions = new();
            var exprInPrms = new ParameterExpression[] { expROMemory, expModel, expPrms, expLogger };

            var miGetString = typeof(UTF8Encoding).GetMethod(nameof(Encoding.UTF8.GetString), [typeof(ReadOnlySpan<byte>)]);
            var miLogTrace = typeof(LoggingExtensions).GetMethod(nameof(LoggingExtensions.TraceMapperGrainPersistance));
            var miSpan = typeof(ReadOnlyMemory<byte>).GetProperty(nameof(ReadOnlyMemory<byte>.Span), BindingFlags.Public | BindingFlags.Instance)!.GetGetMethod()!;

            // Check for ShardKey marked IsRecordIdentifier, return first instance.
            foreach (var prop in tModel.GetProperties())
            {
                Type propType = prop.PropertyType;
                var shdAttr = ExpressionHelpers.GetMapShardKeyAttribute(prop, propType, out var isNullable, out var isShardKey, out var isShardChild, out var isShardGrandChild, out var isShardGreatGrandChild);
                if (shdAttr is not null && shdAttr.IsRecordIdentifier && !isNullable && (isShardKey || isShardChild || isShardGrandChild || isShardGrandChild))
                {
                    var expShardKey = Expression.Property(expModel, typeof(TModel).GetProperty(prop.Name)!.GetGetMethod()!);
                    var miGetShardId = propType.GetProperty(nameof(ShardKey<int>.ShardId), BindingFlags.Public | BindingFlags.Instance)!.GetGetMethod();
                    var expShardId = Expression.Call(expShardKey, miGetShardId!);

                    if (validateKey)
                    {
                        var expToSpan = Expression.Call(expROMemory, miSpan);
                        //var expGrainIdShard = Expression.Call(propType.GetMethod(nameof(ShardKey<int>.FromUtf8), BindingFlags.Static | BindingFlags.Public)!, expToSpan);
                        var expShardNew = Expression.New(propType.GetConstructor([typeof(ReadOnlySpan<byte>)])!, [expToSpan]);
                        var expValid = Expression.IfThen(Expression.NotEqual(expShardKey, expShardNew),
                            Expression.Throw(Expression.New(typeof(OrleansIdKeyMismatchException).GetConstructor([typeof(string)])!, [Expression.Constant(grainType)])));
                        expressions.Add(expValid);
                    }

                    expressions.Add(Expression.Assign(expIgnoreParameters, Expression.New(typeof(HashSet<string>))));

                    var foundPrms = false;
                    var found = ExpressionHelpers.ShardKeyInMapProperties(prop, propType, shdAttr, isNullable, isShardKey, isShardChild, isShardGrandChild, isShardGreatGrandChild, expShardKey, expressions, variables, expPrms, expIgnoreParameters, expLogger, new HashSet<string>(), miLogTrace, ref foundPrms, logger);
                    expressions.Add(expShardId); // return the shardId

                    var inBlock = Expression.Block(variables, expressions);
                    var lmbIn = Expression.Lambda<Func<ReadOnlyMemory<byte>, TModel, ParameterCollection, ILogger?, short>>(inBlock, exprInPrms);
                    logger?.CreatedExpressionTreeForShardKey(grainType, inBlock);
                    return lmbIn.Compile();
                }
            }
            // No explicit key set, now check for non-nullable ShardKey, return first instance.
            foreach (var prop in tModel.GetProperties())
            {
                // identitcal to if block above, except for absense of IsRecordIdentifier check
                Type propType = prop.PropertyType;
                var shdAttr = ExpressionHelpers.GetMapShardKeyAttribute(prop, propType, out var isNullable, out var isShardKey, out var isShardChild, out var isShardGrandChild, out var isShardGreatGrandChild);
                if (shdAttr is not null && !isNullable && (isShardKey || isShardChild || isShardGrandChild || isShardGrandChild))
                {
                    var expShardKey = Expression.Property(expModel, typeof(TModel).GetProperty(prop.Name)!.GetGetMethod()!);
                    var miGetShardId = propType.GetProperty(nameof(ShardKey<int>.ShardId), BindingFlags.Public | BindingFlags.Instance)!.GetGetMethod();
                    var expShardId = Expression.Call(expShardKey, miGetShardId!);

                    if (validateKey)
                    {
                        var expToSpan = Expression.Call(expROMemory, miSpan);
                        //var expGrainIdShard = Expression.Call(propType.GetMethod(nameof(ShardKey<int>.FromUtf8), BindingFlags.Static | BindingFlags.Public)!, expToSpan);
                        var expShardNew = Expression.New(propType.GetConstructor([typeof(ReadOnlySpan<byte>)])!, [expToSpan]);
                        var expValid = Expression.IfThen(Expression.NotEqual(expShardKey, expShardNew),
                            Expression.Throw(Expression.New(typeof(OrleansIdKeyMismatchException).GetConstructor([typeof(string)])!, [Expression.Constant(grainType)])));
                        expressions.Add(expValid);
                    }

                    expressions.Add(Expression.Assign(expIgnoreParameters, Expression.New(typeof(HashSet<string>))));

                    var foundPrms = false;
                    var found = ExpressionHelpers.ShardKeyInMapProperties(prop, propType, shdAttr, isNullable, isShardKey, isShardChild, isShardGrandChild, isShardGreatGrandChild, expShardKey, expressions, variables, expPrms, expIgnoreParameters, expLogger, new HashSet<string>(), miLogTrace, ref foundPrms, logger);
                    expressions.Add(expShardId); // return the shardId

                    var inBlock = Expression.Block(variables, expressions);
                    var lmbIn = Expression.Lambda<Func<ReadOnlyMemory<byte>, TModel, ParameterCollection, ILogger?, short>>(inBlock, exprInPrms);
                    logger?.CreatedExpressionTreeForShardKey(grainType, inBlock);
                    return lmbIn.Compile();
                }
            }
            throw new ShardKeyNotFoundException(grainType);
        }


        internal static Func<TModel, ILogger?, IdSpan> BuildGrainIdFromDbModel<TModel>(ILogger? logger)
        {
            var tModel = typeof(TModel);
            ParameterExpression expModel = Expression.Parameter(typeof(TModel), "model");
            ParameterExpression expLogger = Expression.Parameter(typeof(ILogger), "logger");

            ParameterExpression expSpanLengths = Expression.Variable(typeof(int), "spanLengths");
            ParameterExpression expPosition = Expression.Variable(typeof(int), "position");
            ParameterExpression expBuffer = Expression.Variable(typeof(byte[]), "buffer");
            ParameterExpression expSpan = Expression.Variable(typeof(Span<byte>), "span");
            ParameterExpression expResult = Expression.Variable(typeof(byte[]), "spnResult");

            var variables = new List<ParameterExpression>() { expPosition, expSpanLengths, expBuffer, expSpan, expResult };
            var exprPrms = new ParameterExpression[] { expModel, expLogger };

            var spanSize = 0;
            List<Expression> prologue = [];
            List<Expression> body = [];
            var index = 0;
            var expGetEncoder = Expression.Property(null, typeof(Encoding).GetProperty(nameof(Encoding.UTF8))!);
            var miGetBytes = typeof(UTF8Encoding).GetMethod(nameof(UTF8Encoding.GetBytes), [typeof(string)])!;
            var ciNewSpan = typeof(ReadOnlySpan<byte>).GetConstructor([typeof(byte[])])!;

            var props = tModel.GetProperties().Where(p => p.IsDefined(typeof(ParameterMapAttributeBase), true));

            foreach (var prop in props)
            {
                var attrPM = prop.GetCustomAttribute<ParameterMapAttributeBase>(true);
                if (attrPM is not null && attrPM.IsRecordIdentifier)
                {
                    index++;
                    var tProperty = prop.PropertyType;
                    if (!attrPM.IsValidType(tProperty))
                    {
                        throw new InvalidMapTypeException(prop, attrPM.SqlType, attrPM.SqlTypeName);
                    }
                    var expProp = Expression.Property(expModel, prop);

                    //Handle strings first, to get byte sizes.
                    if (tProperty == typeof(string))
                    {
                        var expBytes = Expression.Variable(typeof(ReadOnlySpan<byte>), $"spn{prop.Name}");
                        variables.Add(expBytes);
                        prologue.Add(Expression.Assign(expBytes, Expression.New(ciNewSpan, [Expression.Call(expGetEncoder, miGetBytes, expProp)])));

                        prologue.Add(Expression.AddAssignChecked(expSpanLengths, Expression.Property(expBytes, nameof(ReadOnlySpan<byte>.Length))));

                        //var position = SpanParser.Append(buffer, position, utf8);
                        var miAppend = typeof(SpanParser).GetMethod(nameof(SpanParser.Append), BindingFlags.Static | BindingFlags.Public, [typeof(Span<byte>), typeof(int), typeof(ReadOnlySpan<byte>)])!;
                        var expCallValue = Expression.Call(miAppend, expSpan, expPosition, expBytes);
                        body.Add(Expression.Assign(expPosition, expCallValue));
                    }
                    else
                    {
                        // var position = SpanParser.Append(buffer, position, value);
                        var miAppend = typeof(SpanParser).GetMethod(nameof(SpanParser.Append), BindingFlags.Static | BindingFlags.Public, [typeof(Span<byte>), typeof(int), tProperty]);
                        if (miAppend is null)
                        {
                            throw new InvalidKeyTypeException(tModel.Name, prop.Name);
                        }
                        var expCallValue = Expression.Call(miAppend, expSpan, expPosition, expProp);
                        body.Add(Expression.Assign(expPosition, expCallValue));
                        if (tProperty == typeof(Guid)) { spanSize += 32; }
                        else if (tProperty == typeof(long) || tProperty == typeof(ulong)) { spanSize += 16; }
                        else if (tProperty == typeof(int) || tProperty == typeof(uint)) { spanSize += 8; }
                        else if (tProperty == typeof(short) || tProperty == typeof(ushort)) { spanSize += 4; }
                        else if (tProperty == typeof(byte) || tProperty == typeof(sbyte)) { spanSize += 2; }
                        else if (tProperty == typeof(decimal)) { spanSize += 32; }

                        if (index > 0) //make room fore the delimiter
                        {
                            spanSize++;
                        }
                    }
                }
            }

            var expTotalSpanSize = Expression.Add(Expression.Constant(spanSize), expSpanLengths);
            prologue.Add(Expression.Assign(expBuffer, Expression.NewArrayBounds(typeof(byte), expTotalSpanSize)));
            prologue.Add(Expression.Assign(expSpan, Expression.New(typeof(Span<byte>).GetConstructor([typeof(byte[])])!, [expBuffer])));


            List<Expression> expressions = prologue;
            expressions.AddRange(body);
            prologue.Add(Expression.Assign(expResult, Expression.NewArrayBounds(typeof(byte), expPosition)));
            var miBufferCopy = typeof(Buffer).GetMethod(nameof(Buffer.BlockCopy), BindingFlags.Static | BindingFlags.Public, [typeof(Array), typeof(int), typeof(Array), typeof(int), typeof(int)])!;
            expressions.Add(Expression.Call(miBufferCopy, [ expBuffer, Expression.Constant(0), expResult, Expression.Constant(0), expPosition ]));
            var expIdSpan = Expression.New(typeof(IdSpan).GetConstructor([typeof(byte[])])!, [expResult]);
            expressions.Add(expIdSpan);

            var block = Expression.Block(variables, expressions);
            var lmb = Expression.Lambda<Func<TModel, ILogger?, IdSpan>>(block, exprPrms);
            logger?.CreatedExpressionTreeForDatabaseKey(tModel.Name, block);
            return lmb.Compile();

        }
        internal static Action<ReadOnlyMemory<byte>, TModel, ILogger?> BuildSetIdFromGrainId<TModel>(string grainType, ILogger? logger)
        {
            var tModel = typeof(TModel);
            ParameterExpression expROMemory = Expression.Parameter(typeof(ReadOnlyMemory<byte>), "roMemory");
            ParameterExpression expModel = Expression.Parameter(typeof(TModel), "model");
            ParameterExpression expLogger = Expression.Parameter(typeof(ILogger), "logger");
            var exprPrms = new ParameterExpression[] { expROMemory, expModel, expLogger };

            ParameterExpression expPosition = Expression.Variable(typeof(int), "position");
            ParameterExpression expSpan = Expression.Variable(typeof(ReadOnlySpan<byte>), "span");
            var miSpan = typeof(ReadOnlyMemory<byte>).GetProperty(nameof(ReadOnlyMemory<byte>.Span), BindingFlags.Public | BindingFlags.Instance)!.GetGetMethod()!;

            List<Expression> expressions = [ Expression.Assign(expSpan, Expression.Call(expROMemory, miSpan)) ];
            List<ParameterExpression> variables = [ expPosition, expSpan ];

            var props = tModel.GetProperties().Where(p => p.IsDefined(typeof(ParameterMapAttributeBase), true));
            foreach (var prop in props)
            {
                bool alreadyFound = false;
                var attrPMs = prop.GetCustomAttributes<ParameterMapAttributeBase>(true);
                foreach (var attrPM in attrPMs)
                {
                    if (alreadyFound)
                    {
                        throw new MultipleMapAttributesException(prop);
                    }
                    alreadyFound = true;

                    if (attrPM.IsRecordIdentifier)
                    {
                        var tProperty = prop.PropertyType;
                        var expProp = Expression.Property(expModel, prop);
                        if (!attrPM.IsValidType(tProperty))
                        {
                            throw new InvalidMapTypeException(prop, attrPM.SqlType, attrPM.SqlTypeName);
                        }
                        // span = span.Extract(out model.prop);
                        var miCall = typeof(ArgentSea.Orleans.SpanParser).GetMethod(nameof(SpanParser.Extract), BindingFlags.Static | BindingFlags.Public | BindingFlags.ExactBinding, [typeof(ReadOnlySpan<byte>), tProperty.MakeByRefType() ]);
                        if (miCall is null)
                        {
                            throw new InvalidKeyTypeException(grainType, prop.Name);
                        }
                        var expCall = Expression.Call(miCall, expSpan, expProp);
                        expressions.Add(Expression.Assign(expSpan, expCall));
                    }
                }
            }
            var expBlock = Expression.Block(variables, expressions);
            var lmbOut = Expression.Lambda<Action<ReadOnlyMemory<byte>, TModel, ILogger?>>(expBlock, exprPrms);
            logger?.CreatedExpressionTreeForActivation(grainType, expBlock);
            return lmbOut.Compile();

        }
    }
}
