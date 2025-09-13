using ArgentSea.Orleans;

namespace ArgentSea.Orleans.Test
{
    internal static class Queries
    {
        // Example Subscriber Queries
        public static QueryProcedure GetSubscriber => new QueryProcedure("ws.ReadSubscriberV1", new[] { "SubscriberKey" });

        private static readonly Lazy<QueryStatement> _writeSubscriber = QueryStatement.Create("WriteSubscriberV1", new[] { "SubscriberKey" });
        
        public static QueryStatement WriteSubscriber => _writeSubscriber.Value;

        public static QueryProcedure ClearSubscriber => new QueryProcedure("ws.ClearSubscriberV1", new[] { "SubscriberKey" });

        internal static OrleansShardQueryDefinitions SubscriberQueries = new("SubscriberGrain", GetSubscriber, QueryResultFormat.ResultSet, _writeSubscriber, ClearSubscriber);


        // Example Product Queries
        public static QueryProcedure GetProduct => new QueryProcedure("ws.ReadProductV1", new[] { "ProductKey" });

        private static readonly Lazy<QueryStatement> _writeProduct = QueryStatement.Create("WriteProductV1", new[] { "ProductKey" });
        public static QueryStatement WriteProduct => _writeProduct.Value;

        public static QueryProcedure ClearProduct => new QueryProcedure("ws.ClearProductV1", new[] { "ProductKey" });

        internal static OrleansShardQueryDefinitions ProductQueries = new("ProductGrain", GetProduct, QueryResultFormat.ResultSet, _writeProduct, ClearProduct);

        // Example User Queries
        public static QueryProcedure GetUser => new QueryProcedure("ws.ReadUserV1", new[] { "userid" });
        public static QueryProcedure WriteUser => new QueryProcedure("ws.WriteUserV1", new[] { "userid" });
        public static QueryProcedure ClearUser => new QueryProcedure("ws.ClearUserV1", new[] { "Userid" });

        internal static OrleansDbQueryDefinitions UserQueries = new("UserGrain", GetUser, QueryResultFormat.ResultSet, WriteUser, ClearUser);

        public static List<OrleansDbQueryDefinitions> AllDbQueryDefinitions = new() { 
            { UserQueries }
        };

        public static List<OrleansShardQueryDefinitions> AllShardQueryDefinitions = new() {
            { SubscriberQueries },
            { ProductQueries }
        };

    }


}
