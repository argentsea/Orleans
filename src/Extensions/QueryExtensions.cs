using ArgentSea;
using ArgentSea.ShardKeys;
using static System.Runtime.InteropServices.JavaScript.JSType;

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
        var gk = new GhostShardKey(grainId.Key.Value);
        return gk.GetShardId();
    }

}