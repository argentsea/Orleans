using System.Buffers;
using ArgentSea;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Cloning;
using Orleans.Serialization.Codecs;
using Orleans.Serialization.WireProtocol;

namespace ArgentSea.Orleans;

/// <summary>
/// This class registers a ShardKey&lt;Guid&gt; type with Orleans. This is likely the majority of scenarios. Other types require a custom registration.
/// </summary>
[RegisterSerializer]
[RegisterCopier]
public class ShardKeyGuidOrleansSerializer : IFieldCodec<ShardKey<Guid>>, IDeepCopier<ShardKey<Guid>>
{
    public ShardKey<Guid> DeepCopy(ShardKey<Guid> input, CopyContext context)
    {
        return new ShardKey<Guid>(input.ToArray());
    }

    public ShardKey<Guid> ReadValue<TInput>(ref Reader<TInput> reader, Field field)
    {
        if (field.WireType == WireType.Reference)
        {
            return ReferenceCodec.ReadReference<ShardKey<Guid>, TInput>(ref reader, field);
        }

        field.EnsureWireType(WireType.LengthPrefixed);
        var numBytes = reader.ReadByte();
        ReadOnlySpan<byte> resultArray = reader.ReadBytes(numBytes);
        var shardKey = new ShardKey<Guid>(resultArray);

        ReferenceCodec.RecordObject(reader.Session, shardKey);
        return shardKey;
    }

    public void WriteField<TBufferWriter>(ref Writer<TBufferWriter> writer, uint fieldIdDelta, Type expectedType, ShardKey<Guid> value) where TBufferWriter : IBufferWriter<byte>
    {
        if (ReferenceCodec.TryWriteReferenceField(ref writer, fieldIdDelta, expectedType, value))
        {
            return;
        }

        writer.WriteFieldHeader(fieldIdDelta, expectedType, typeof(ShardKey<Guid>), WireType.LengthPrefixed);
        var bytes = value.ToArray();
        writer.WriteByte((byte)bytes.Length);
        writer.Write(bytes);
    }
}