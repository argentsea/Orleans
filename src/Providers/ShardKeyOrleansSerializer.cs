using System.Buffers;
using ArgentSea;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Cloning;
using Orleans.Serialization.Codecs;
using Orleans.Serialization.WireProtocol;

namespace ArgentSea.Orleans;

/// <summary>
/// This class registers a ShardKey&lt;T&gt; type with Orleans.
/// This must be loaded at startup with a singleton instance for every type that is in use in the Orleans application.
/// </summary>
[RegisterSerializer]
[RegisterCopier]
public class ShardKeyOrleansSerializer<T> : IFieldCodec<ShardKey<T>>, IDeepCopier<ShardKey<T>> where T : IComparable
{
    public ShardKey<T> DeepCopy(ShardKey<T> input, CopyContext context)
    {
        return new ShardKey<T>(input.ToArray());
    }

    public ShardKey<T> ReadValue<TInput>(ref Reader<TInput> reader, Field field)
    {
        if (field.WireType == WireType.Reference)
        {
            return ReferenceCodec.ReadReference<ShardKey<T>, TInput>(ref reader, field);
        }

        field.EnsureWireType(WireType.LengthPrefixed);
        var numBytes = reader.ReadByte();
        ReadOnlySpan<byte> resultArray = reader.ReadBytes(numBytes);
        var shardKey = new ShardKey<T>(resultArray);

        ReferenceCodec.RecordObject(reader.Session, shardKey);
        return shardKey;
    }

    public void WriteField<TBufferWriter>(ref Writer<TBufferWriter> writer, uint fieldIdDelta, Type expectedType, ShardKey<T> value) where TBufferWriter : IBufferWriter<byte>
    {
        if (ReferenceCodec.TryWriteReferenceField(ref writer, fieldIdDelta, expectedType, value))
        {
            return;
        }

        writer.WriteFieldHeader(fieldIdDelta, expectedType, typeof(ShardKey<T>), WireType.LengthPrefixed);
        var bytes = value.ToArray();
        writer.WriteByte((byte)bytes.Length);
        writer.Write(bytes);
    }
}

public class ShardKeyOrleansSerializer<TShard, TChild> : IFieldCodec<ShardKey<TShard, TChild>>, IDeepCopier<ShardKey<TShard, TChild>> where TShard : IComparable where TChild : IComparable
{
    public ShardKey<TShard, TChild> DeepCopy(ShardKey<TShard, TChild> input, CopyContext context)
    {
        return new ShardKey<TShard, TChild>(input.ToArray());
    }

    public ShardKey<TShard, TChild> ReadValue<TInput>(ref Reader<TInput> reader, Field field)
    {
        if (field.WireType == WireType.Reference)
        {
            return ReferenceCodec.ReadReference<ShardKey<TShard, TChild>, TInput>(ref reader, field);
        }

        field.EnsureWireType(WireType.LengthPrefixed);
        var numBytes = reader.ReadByte();
        ReadOnlySpan<byte> resultArray = reader.ReadBytes(numBytes);
        var shardKey = new ShardKey<TShard, TChild>(resultArray);

        ReferenceCodec.RecordObject(reader.Session, shardKey);
        return shardKey;
    }

    public void WriteField<TBufferWriter>(ref Writer<TBufferWriter> writer, uint fieldIdDelta, Type expectedType, ShardKey<TShard, TChild> value) where TBufferWriter : IBufferWriter<byte>
    {
        if (ReferenceCodec.TryWriteReferenceField(ref writer, fieldIdDelta, expectedType, value))
        {
            return;
        }

        writer.WriteFieldHeader(fieldIdDelta, expectedType, typeof(ShardKey<TShard, TChild>), WireType.LengthPrefixed);
        var bytes = value.ToArray();
        writer.WriteByte((byte)bytes.Length);
        writer.Write(bytes);
    }
}

public class ShardKeyOrleansSerializer<TShard, TChild, TGrandChild> : IFieldCodec<ShardKey<TShard, TChild, TGrandChild>>, IDeepCopier<ShardKey<TShard, TChild, TGrandChild>> where TShard : IComparable where TChild : IComparable where TGrandChild : IComparable
{
    public ShardKey<TShard, TChild, TGrandChild> DeepCopy(ShardKey<TShard, TChild, TGrandChild> input, CopyContext context)
    {
        return new ShardKey<TShard, TChild, TGrandChild>(input.ToArray());
    }

    public ShardKey<TShard, TChild, TGrandChild> ReadValue<TInput>(ref Reader<TInput> reader, Field field)
    {
        if (field.WireType == WireType.Reference)
        {
            return ReferenceCodec.ReadReference<ShardKey<TShard, TChild, TGrandChild>, TInput>(ref reader, field);
        }

        field.EnsureWireType(WireType.LengthPrefixed);
        var numBytes = reader.ReadByte();
        ReadOnlySpan<byte> resultArray = reader.ReadBytes(numBytes);
        var shardKey = new ShardKey<TShard, TChild, TGrandChild>(resultArray);

        ReferenceCodec.RecordObject(reader.Session, shardKey);
        return shardKey;
    }

    public void WriteField<TBufferWriter>(ref Writer<TBufferWriter> writer, uint fieldIdDelta, Type expectedType, ShardKey<TShard, TChild, TGrandChild> value) where TBufferWriter : IBufferWriter<byte>
    {
        if (ReferenceCodec.TryWriteReferenceField(ref writer, fieldIdDelta, expectedType, value))
        {
            return;
        }

        writer.WriteFieldHeader(fieldIdDelta, expectedType, typeof(ShardKey<TShard, TChild, TGrandChild>), WireType.LengthPrefixed);
        var bytes = value.ToArray();
        writer.WriteByte((byte)bytes.Length);
        writer.Write(bytes);
    }
}

public class ShardKeyOrleansSerializer<TShard, TChild, TGrandChild, TGreatGrandChild> : IFieldCodec<ShardKey<TShard, TChild, TGrandChild, TGreatGrandChild>>, IDeepCopier<ShardKey<TShard, TChild, TGrandChild, TGreatGrandChild>> where TShard : IComparable where TChild : IComparable where TGrandChild : IComparable where TGreatGrandChild : IComparable
{
    public ShardKey<TShard, TChild, TGrandChild, TGreatGrandChild> DeepCopy(ShardKey<TShard, TChild, TGrandChild, TGreatGrandChild> input, CopyContext context)
    {
        return new ShardKey<TShard, TChild, TGrandChild, TGreatGrandChild>(input.ToArray());
    }

    public ShardKey<TShard, TChild, TGrandChild, TGreatGrandChild> ReadValue<TInput>(ref Reader<TInput> reader, Field field)
    {
        if (field.WireType == WireType.Reference)
        {
            return ReferenceCodec.ReadReference<ShardKey<TShard, TChild, TGrandChild, TGreatGrandChild>, TInput>(ref reader, field);
        }

        field.EnsureWireType(WireType.LengthPrefixed);
        var numBytes = reader.ReadByte();
        ReadOnlySpan<byte> resultArray = reader.ReadBytes(numBytes);
        var shardKey = new ShardKey<TShard, TChild, TGrandChild, TGreatGrandChild>(resultArray);

        ReferenceCodec.RecordObject(reader.Session, shardKey);
        return shardKey;
    }

    public void WriteField<TBufferWriter>(ref Writer<TBufferWriter> writer, uint fieldIdDelta, Type expectedType, ShardKey<TShard, TChild, TGrandChild, TGreatGrandChild> value) where TBufferWriter : IBufferWriter<byte>
    {
        if (ReferenceCodec.TryWriteReferenceField(ref writer, fieldIdDelta, expectedType, value))
        {
            return;
        }

        writer.WriteFieldHeader(fieldIdDelta, expectedType, typeof(ShardKey<TShard, TChild, TGrandChild, TGreatGrandChild>), WireType.LengthPrefixed);
        var bytes = value.ToArray();
        writer.WriteByte((byte)bytes.Length);
        writer.Write(bytes);
    }
}
