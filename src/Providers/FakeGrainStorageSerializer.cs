using Orleans.Storage;

namespace ArgentSea.Orleans;

public class FakeGrainStorageSerializer : IGrainStorageSerializer
{
    public T Deserialize<T>(BinaryData input) => default!;
    public BinaryData Serialize<T>(T input) => BinaryData.Empty;
}