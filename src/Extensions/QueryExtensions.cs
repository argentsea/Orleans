using ArgentSea.ShardKeys;

namespace ArgentSea.Orleans;

public static class QueryExtensions
{
    /// <summary>
    /// Works on any sharded grain identity (i.e. ShardKey;gt;Guid;lt, or ShardKey;gt;long;lt, etc.
    /// </summary>
    /// <param name="grainId">The Orleans grain identity.</param>
    /// <returns>Int16 shard Id.</returns>
    public static short ShardId(this GrainId grainId)
    {
        if (TryGetFromExternalString(grainId, out var externalKeyShardId))
        {
            return externalKeyShardId;
        }

        var gk = new GhostShardKey(grainId.Key.Value);
        var shardKey = gk.GetShardId();
        return shardKey;
    }

    private static bool TryGetFromExternalString(GrainId grainId, out short shardId)
    {
        try
        {
            var shardKey = ShardKey<string>.FromExternalString(grainId.Key.ToString());
            shardId = shardKey.ShardId;
            return true;
        }
        catch
        {
            shardId = -1;
            return false;
        }
    }
}