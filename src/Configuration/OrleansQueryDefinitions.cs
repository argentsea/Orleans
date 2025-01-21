// © John Hicks. All rights reserved. Licensed under the MIT license.
// See the LICENSE file in the repository root for more information.

using Orleans.Storage;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace ArgentSea.Orleans
{

    public enum QueryResultFormat
    {
        OutputArgs,
        ResultSet

    }

    /// <summary>
    /// Each instance of this struct defines the SQL queries for a single grain type.
    /// </summary>
    public struct OrleansDbQueryDefinitions
    {
        private readonly string grainType;

        private readonly QueryProcedure? readQuery;
        private readonly Lazy<QueryStatement>? lazyReadQuery;
        private readonly QueryResultFormat resultType;

        private readonly QueryProcedure? writeQuery;
        private readonly Lazy<QueryStatement>? lazyWriteQuery;

        private readonly QueryProcedure? clearQuery;
        private readonly Lazy<QueryStatement>? lazyClearQuery;

        #region Constructors
        public OrleansDbQueryDefinitions(string grainType, QueryProcedure readQuery, QueryResultFormat resultType, QueryProcedure writeQuery, QueryProcedure cleardQuery)
        {
            this.grainType = grainType;
            this.readQuery = readQuery;
            this.resultType = resultType;
            this.writeQuery = writeQuery;
            this.clearQuery = cleardQuery;
            this.lazyReadQuery = null;
            this.lazyWriteQuery = null;
            this.lazyClearQuery = null;
        }

        public OrleansDbQueryDefinitions(string grainType, Lazy<QueryStatement> readQuery, QueryResultFormat resultType, QueryProcedure writeQuery, QueryProcedure cleardQuery)
        {
            this.grainType = grainType;
            this.readQuery = null;
            this.resultType = resultType;
            this.writeQuery = writeQuery;
            this.clearQuery = cleardQuery;
            this.lazyReadQuery = readQuery;
            this.lazyWriteQuery = null;
            this.lazyClearQuery = null;
        }

        public OrleansDbQueryDefinitions(string grainType, QueryProcedure readQuery, QueryResultFormat resultType, Lazy<QueryStatement> writeQuery, QueryProcedure cleardQuery)
        {
            this.grainType = grainType;
            this.readQuery = readQuery;
            this.resultType = resultType;
            this.writeQuery = null;
            this.clearQuery = cleardQuery;
            this.lazyReadQuery = null;
            this.lazyWriteQuery = writeQuery;
            this.lazyClearQuery = null;
        }

        public OrleansDbQueryDefinitions(string grainType, Lazy<QueryStatement> readQuery, QueryResultFormat resultType, Lazy<QueryStatement> writeQuery, QueryProcedure cleardQuery)
        {
            this.grainType = grainType;
            this.readQuery = null;
            this.resultType = resultType;
            this.writeQuery = null;
            this.clearQuery = cleardQuery;
            this.lazyReadQuery = readQuery;
            this.lazyWriteQuery = writeQuery;
            this.lazyClearQuery = null;
        }


        public OrleansDbQueryDefinitions(string grainType, QueryProcedure readQuery, QueryResultFormat resultType, QueryProcedure writeQuery, Lazy<QueryStatement> cleardQuery)
        {
            this.grainType = grainType;
            this.readQuery = readQuery;
            this.resultType = resultType;
            this.writeQuery = writeQuery;
            this.clearQuery = null;
            this.lazyReadQuery = null;
            this.lazyWriteQuery = null;
            this.lazyClearQuery = cleardQuery;
        }
        public OrleansDbQueryDefinitions(string grainType, Lazy<QueryStatement> readQuery, QueryResultFormat resultType, QueryProcedure writeQuery, Lazy<QueryStatement> cleardQuery)
        {
            this.grainType = grainType;
            this.readQuery = null;
            this.resultType = resultType;
            this.writeQuery = writeQuery;
            this.clearQuery = null;
            this.lazyReadQuery = readQuery;
            this.lazyWriteQuery = null;
            this.lazyClearQuery = cleardQuery;
        }

        public OrleansDbQueryDefinitions(string grainType, QueryProcedure readQuery, QueryResultFormat resultType, Lazy<QueryStatement> writeQuery, Lazy<QueryStatement> cleardQuery)
        {
            this.grainType = grainType;
            this.readQuery = readQuery;
            this.resultType = resultType;
            this.writeQuery = null;
            this.clearQuery = null;
            this.lazyReadQuery = null;
            this.lazyWriteQuery = writeQuery;
            this.lazyClearQuery = cleardQuery;
        }

        public OrleansDbQueryDefinitions(string grainType, Lazy<QueryStatement> readQuery, QueryResultFormat resultType, Lazy<QueryStatement> writeQuery, Lazy<QueryStatement> cleardQuery)
        {
            this.grainType = grainType;
            this.readQuery = null;
            this.resultType = resultType;
            this.writeQuery = null;
            this.clearQuery = null;
            this.lazyReadQuery = readQuery;
            this.lazyWriteQuery = writeQuery;
            this.lazyClearQuery = cleardQuery;
        }
        #endregion

        public Query ReadQuery
        {
            get => (Query?)this.readQuery ?? (Query?)this.lazyReadQuery?.Value ?? throw new OrleansQueryNotProvidedException(this.grainType);
        }

        public Query WriteQuery
        {
            get => (Query?)this.writeQuery ?? (Query?)this.lazyWriteQuery?.Value ?? throw new OrleansQueryNotProvidedException(this.grainType);
        }

        public Query ClearQuery
        {
            get => (Query?)this.readQuery ?? (Query?)this.lazyReadQuery?.Value ?? throw new OrleansQueryNotProvidedException(this.grainType);
        }

        public string GrainType { get => this.grainType; }

        public QueryResultFormat ResultFormat { get => this.resultType; }
    }

    /// <summary>
    /// Each instance of this struct defines the SQL queries for a single grain type.
    /// </summary>
    public struct OrleansShardQueryDefinitions
    {
        private readonly string grainType;

        private readonly QueryProcedure? readQuery;
        private readonly Lazy<QueryStatement>? lazyReadQuery;
        private readonly QueryResultFormat resultType;

        private readonly QueryProcedure? writeQuery;
        private readonly Lazy<QueryStatement>? lazyWriteQuery;

        private readonly QueryProcedure? clearQuery;
        private readonly Lazy<QueryStatement>? lazyClearQuery;

        //private readonly ParameterMapAttributeBase? concurrency = null;
        private readonly char origin;

        #region Constructors
        public OrleansShardQueryDefinitions(string grainType, char origin, QueryProcedure readQuery, QueryResultFormat resultType, QueryProcedure writeQuery, QueryProcedure cleardQuery)
        {
            this.grainType = grainType;
            this.origin = origin;
            this.readQuery = readQuery;
            this.resultType = resultType;
            this.writeQuery = writeQuery;
            this.clearQuery = cleardQuery;
            this.lazyReadQuery = null;
            this.lazyWriteQuery = null;
            this.lazyClearQuery = null;
        }

        public OrleansShardQueryDefinitions(string grainType, char origin, Lazy<QueryStatement> readQuery, QueryResultFormat resultType, QueryProcedure writeQuery, QueryProcedure cleardQuery)
        {
            this.grainType = grainType;
            this.origin = origin;
            this.readQuery = null;
            this.resultType = resultType;
            this.writeQuery = writeQuery;
            this.clearQuery = cleardQuery;
            this.lazyReadQuery = readQuery;
            this.lazyWriteQuery = null;
            this.lazyClearQuery = null;
        }

        public OrleansShardQueryDefinitions(string grainType, char origin, QueryProcedure readQuery, QueryResultFormat resultType, Lazy<QueryStatement> writeQuery, QueryProcedure cleardQuery)
        {
            this.grainType = grainType;
            this.origin = origin;
            this.readQuery = readQuery;
            this.resultType = resultType;
            this.writeQuery = null;
            this.clearQuery = cleardQuery;
            this.lazyReadQuery = null;
            this.lazyWriteQuery = writeQuery;
            this.lazyClearQuery = null;
        }

        public OrleansShardQueryDefinitions(string grainType, char origin, Lazy<QueryStatement> readQuery, QueryResultFormat resultType, Lazy<QueryStatement> writeQuery, QueryProcedure cleardQuery)
        {
            this.grainType = grainType;
            this.origin = origin;
            this.readQuery = null;
            this.resultType = resultType;
            this.writeQuery = null;
            this.clearQuery = cleardQuery;
            this.lazyReadQuery = readQuery;
            this.lazyWriteQuery = writeQuery;
            this.lazyClearQuery = null;
        }


        public OrleansShardQueryDefinitions(string grainType, char origin, QueryProcedure readQuery, QueryResultFormat resultType, QueryProcedure writeQuery, Lazy<QueryStatement> cleardQuery)
        {
            this.grainType = grainType;
            this.origin = origin;
            this.readQuery = readQuery;
            this.resultType = resultType;
            this.writeQuery = writeQuery;
            this.clearQuery = null;
            this.lazyReadQuery = null;
            this.lazyWriteQuery = null;
            this.lazyClearQuery = cleardQuery;
        }
        public OrleansShardQueryDefinitions(string grainType, char origin, Lazy<QueryStatement> readQuery, QueryResultFormat resultType, QueryProcedure writeQuery, Lazy<QueryStatement> cleardQuery)
        {
            this.grainType = grainType;
            this.origin = origin;
            this.readQuery = null;
            this.resultType = resultType;
            this.writeQuery = writeQuery;
            this.clearQuery = null;
            this.lazyReadQuery = readQuery;
            this.lazyWriteQuery = null;
            this.lazyClearQuery = cleardQuery;
        }

        public OrleansShardQueryDefinitions(string grainType, char origin, QueryProcedure readQuery, QueryResultFormat resultType, Lazy<QueryStatement> writeQuery, Lazy<QueryStatement> cleardQuery)
        {
            this.grainType = grainType;
            this.origin = origin;
            this.readQuery = readQuery;
            this.resultType = resultType;
            this.writeQuery = null;
            this.clearQuery = null;
            this.lazyReadQuery = null;
            this.lazyWriteQuery = writeQuery;
            this.lazyClearQuery = cleardQuery;
        }

        public OrleansShardQueryDefinitions(string grainType, char origin, Lazy<QueryStatement> readQuery, QueryResultFormat resultType, Lazy<QueryStatement> writeQuery, Lazy<QueryStatement> cleardQuery)
        {
            this.grainType = grainType;
            this.origin = origin;
            this.readQuery = null;
            this.resultType = resultType;
            this.writeQuery = null;
            this.clearQuery = null;
            this.lazyReadQuery = readQuery;
            this.lazyWriteQuery = writeQuery;
            this.lazyClearQuery = cleardQuery;
        }
        #endregion

        public Query ReadQuery
        {
            get => (Query?)this.readQuery ?? (Query?)this.lazyReadQuery?.Value ?? throw new OrleansQueryNotProvidedException(this.grainType);
        }

        public Query WriteQuery
        {
            get => (Query?)this.writeQuery ?? (Query?)this.lazyWriteQuery?.Value ?? throw new OrleansQueryNotProvidedException(this.grainType);
        }

        public Query ClearQuery
        {
            get => (Query?)this.readQuery ?? (Query?)this.lazyReadQuery?.Value ?? throw new OrleansQueryNotProvidedException(this.grainType);
        }

        public string GrainType { get => this.grainType; }

        public QueryResultFormat ResultFormat { get => this.resultType; }

        //public ParameterMapAttributeBase? Concurrency { get => this.concurrency; }

        public char Origin { get => this.origin; }
    }
}