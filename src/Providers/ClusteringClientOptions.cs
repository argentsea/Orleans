
public sealed record ClusteringClientOptions
{
    /// <summary>
    /// Configuration for the clent to connect to the customering Db.
    /// </summary>
    [Redact]
    public required string ConnectionServer { get; set; }

    public required string ConnectionDatabase { get; set; }

    public required string ConnectionUsername { get; set; }

    public required string ConnectionPassword { get; set; }

    public required string ProcedureName { get; set; }

    public TimeSpan MaxRefreshInterval { get; set; } = TimeSpan.FromSeconds(60);

    public required string ClusterId { get; set; }
 
    public bool TrustServerCertificate { get; set; } = false;

}