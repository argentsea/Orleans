using ArgentSea;
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
        Span<byte> aValue = StringExtensions.Decode(grainId.Key.AsSpan());
        var orgnLen = aValue[0] & 3;
        var pos = orgnLen + 3;
        return BitConverter.ToInt16(aValue.Slice(pos));
    }

    /// <summary>
    /// Works ONLY on sharded grain identiies of type ShardKey;gt;Guid;lt.
    /// </summary>
    /// <param name="grainId"></param>
    /// <returns></returns>
    public static char Origin(this GrainId grainId)
    {
        Span<byte> aValue = StringExtensions.Decode(grainId.Key.AsSpan());
        var orgnLen = aValue[0] & 3;
        return System.Text.Encoding.UTF8.GetString(aValue.Slice(1, orgnLen))[0];
    }

    public static (char origin, short shardId) OriginAndShardId(this GrainId grainId)
    {
        Span<byte> aValue = StringExtensions.Decode(grainId.Key.AsSpan());
        var orgnLen = aValue[0] & 3;
        var origin = System.Text.Encoding.UTF8.GetString(aValue.Slice(1, orgnLen))[0];
        var pos = orgnLen + 3;
        var shardId = BitConverter.ToInt16(aValue.Slice(pos));
        return (origin, shardId);
    }

}