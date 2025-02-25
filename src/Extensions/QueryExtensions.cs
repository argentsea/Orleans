using ArgentSea;

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
    public static Guid GetGuid(this GrainId grainId)
    {
        var shd = ShardKey<Guid>.FromUtf8(grainId.Key.Value.Span);
        return shd.RecordId;
    }
}