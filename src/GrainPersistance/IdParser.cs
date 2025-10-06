//using ArgentSea.ShardKeys;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//namespace ArgentSea.Orleans;

//public class IdParser
//{
//    private readonly GrainId _grainId;
//    public IdParser(GrainId grainId)
//    {
//        _grainId = grainId;
//    }
//    public string ToString()
//    {
//        var id = new GhostShardKey(_grainId.Key.Value);
//        return id.ToString() + ", \"type\": " + _grainId.Type.ToString();
//    }
//}

//public class IdParser2
//{
//    private readonly IdSpan _idSpan;
//    public IdParser2(IdSpan idSpan)
//    {
//        _idSpan = idSpan;
//    }
//    public string ToString()
//    {
//        var id = new GhostShardKey(_idSpan.Value);
//        return id.ToString();
//    }
//}
