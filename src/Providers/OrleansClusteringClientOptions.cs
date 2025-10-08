using ArgentSea;

/// <summary>
/// Configuration for cluster clients to connect to the database to fetch cluster info.
/// </summary>
public sealed record ClusterClientDbOptions
{
    /// <summary>
    /// The DatabaseKey for the DbConnection in the ArgentSea Db Connections array. Defaults to “Common”.
    /// </summary>
    public required string DatabaseKey { get; set; } = "Common";

    /// <summary>
    /// The connection string IF THE ARGENTSEA DBCOLLECTION IS NOT USED. Otherwise, Null will cause the system to use the DatabaseKey connect via the ArgentSea DbCollection.
    /// </summary>
    public string? ConnectionString { get; set; } = null;


    /// <summary>
    /// The query to retrieve the cluster data.
    /// </summary>
    public required Query GatewayListQuery { get; set; } = new QueryProcedure("rdr.OrleansClusterGatewayListV1", ["@ClusterId"]);

    /// <summary>
    /// How often to query for changes to the cluster set.
    /// </summary>
    public TimeSpan MaxRefreshInterval { get; set; } = TimeSpan.FromSeconds(60);
}
