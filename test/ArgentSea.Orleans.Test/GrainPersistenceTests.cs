using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Options;
using ArgentSea.Orleans;
using Orleans.Configuration;
using Microsoft.Extensions.Logging;
using NSubstitute;
using System.Collections.Concurrent;
using ArgentSea.Sql;


namespace ArgentSea.Orleans.Test
{
    public class GrainPersistenceTests
    {
        private class TestDbModel
        {
            [MapToSqlUniqueIdentifier("ArgentSeaTestDataId1", true)]
            public Guid Id1 { get; set; } = Guid.NewGuid();

            [MapToSqlBigInt("ArgentSeaTestDataId2", true)]
            public long Id2 { get; set; } = 1234L;

            [MapToSqlNVarChar("ArgentSeaTestDataName1", 150, true)]
            public string Name1 { get; set; } = string.Empty;

            [MapToSqlNVarChar("ArgentSeaTestDataName2", 150, true)]
            public string Name2 { get; set; } = "This is a test";

            [MapToSqlNVarChar("ArgentSeaTestDataName", -1)]
            public string? Description { get; set; }
        }


        private class TestShardModel
        {
            [MapSqlShardKey("DataShard", "DataRecordId")]
            [MapToSqlInt("DataRecordId")]
            public ShardKey<int>? RecordKey { get; set; } = ShardKey<int>.Empty;

            [MapSqlShardKey("ChildShard", "ParentRecordId", "ChildRecordId")]
            [MapToSqlInt("ParentRecordId")]
            [MapToSqlSmallInt("ChildRecordId")]
            public ShardKey<int, short> RecordChild { get; set; } = new ShardKey<int, short>(3, 123, 4321);


            [MapSqlShardKey("@DataRecordId2")]
            [MapToSqlBigInt("@DataRecordId2")]
            public ShardKey<long> DataShard2 { get; set; } = ShardKey<long>.Empty; 

            [MapSqlShardKey("ChildShard2", "ParentRecord2Id", "ChildRecord2Id")]
            [MapToSqlSmallInt("ParentRecord2Id")]
            [MapToSqlNVarChar("ChildRecord2Id", 255)]
            public ShardKey<short, string>? ChildShard2 { get; set; } = null;


            [MapToSqlUniqueIdentifier("ArgentSeaTestDataId1", true)]
            public Guid Id1 { get; set; } = Guid.NewGuid();

            [MapToSqlBigInt("ArgentSeaTestDataId2", true)]
            public long Id2 { get; set; } = 1234L;

            [MapToSqlNVarChar("ArgentSeaTestDataName1", 150, true)]
            public string Name1 { get; set; } = string.Empty;

            [MapToSqlNVarChar("ArgentSeaTestDataName2", 150, true)]
            public string Name2 { get; set; } = "This is a test";

            [MapToSqlNVarChar("ArgentSeaTestDataName", -1)]
            public string? Description { get; set; }
        }

        private class TestShardModel2 // this time without the isRecordIdentifier flag set
        {
            [MapSqlShardKey("DataShard", "DataRecordId")]
            [MapToSqlInt("DataRecordId")]
            public ShardKey<int>? RecordKey { get; set; } = ShardKey<int>.Empty;

            [MapSqlShardKey("ChildShard","ParentRecordId", "ChildRecordId")]
            [MapToSqlInt("ParentRecordId")]
            [MapToSqlSmallInt("ChildRecordId")]
            public ShardKey<int, short> RecordChild { get; set; } = new ShardKey<int, short>(3, 123, 4321);


            [MapSqlShardKey("@DataRecordId2")]
            [MapToSqlBigInt("@DataRecordId2")]
            public ShardKey<long> DataShard2 { get; set; } = ShardKey<long>.Empty;

            [MapSqlShardKey("ChildShard2", "ParentRecord2Id", "ChildRecord2Id")]
            [MapToSqlSmallInt("ParentRecord2Id")]
            [MapToSqlNVarChar("ChildRecord2Id", 255)]
            public ShardKey<short, string>? ChildShard2 { get; set; } = null;


            [MapToSqlUniqueIdentifier("ArgentSeaTestDataId1", true)]
            public Guid Id1 { get; set; } = Guid.NewGuid();

            [MapToSqlBigInt("ArgentSeaTestDataId2", true)]
            public long Id2 { get; set; } = 1234L;
        }

        [Fact]
        public void TestDbGetParamsLambda()
        {
            var lmbaGetGrainId = OrleansExpressionHelper.BuildGrainIdFromDbModel<TestDbModel>(new DebugLogger());
            var lmbaRead = OrleansExpressionHelper.BuildDbReadLambda<TestDbModel>("test", new DebugLogger());

            var model1 = new TestDbModel();
            var span = lmbaGetGrainId(model1, new DebugLogger());

            var prms = new ParameterCollection();
            lmbaRead(span.Value, prms, new DebugLogger());

            prms.Count.Should().Be(4);
            model1.Id1.Should().Be((Guid)prms["@ArgentSeaTestDataId1"].Value);
            model1.Id2.Should().Be((long)prms["@ArgentSeaTestDataId2"].Value);
            model1.Name1.Should().Be((string)prms["@ArgentSeaTestDataName1"].Value);
            model1.Name2.Should().Be((string)prms["@ArgentSeaTestDataName2"].Value);

        }

        [Fact]
        public void TestDbCreateParamsLambda()
        {
            var lmbaGetGrainId = OrleansExpressionHelper.BuildGrainIdFromDbModel<TestDbModel>(new DebugLogger());
            var lmbaSetId = OrleansExpressionHelper.BuildSetIdFromGrainId<TestDbModel>("test", new DebugLogger());
            
            var model1 = new TestDbModel();
            var span = lmbaGetGrainId(model1, new DebugLogger());

            var model2 = new TestDbModel();
            model2.Id1 = Guid.NewGuid();
            model2.Id2 = 3462643L;
            model2.Name1 = "Deifferent name";
            model2.Name2 = "Another name";
            model2.Description = "No description";

            lmbaSetId(span.Value, model2, new DebugLogger());

            model1.Id1.Should().Be(model2.Id1);
            model1.Id2.Should().Be(model2.Id2);
            model1.Name1.Should().Be(model2.Name1);
            model1.Name2.Should().Be(model2.Name2);
            model1.Description.Should().NotBe(model2.Description);
        }

        [Fact]
        public void TestSetShardKeyLambda()
        {
            var shd = new ShardKey<int, short>(3, 123, 4321); // new ShardKey<int, short>('y', 1, 5678, 9876);
            var utf8Shard = shd.ToUtf8();
            var lmbaSetShard = OrleansExpressionHelper.BuildSetShardLambda<TestShardModel>("test", new DebugLogger());
            var model = new TestShardModel();
            lmbaSetShard(utf8Shard, model, new DebugLogger());
            model.RecordChild.Should().Be(shd);
        }

        [Fact]
        public void TestShardReadLambda()
        {
            var lmbaRead = OrleansExpressionHelper.BuildShardReadLambda<TestShardModel>("test", new DebugLogger());
            var lmbaSetShard = OrleansExpressionHelper.BuildSetShardLambda<TestShardModel>("test", new DebugLogger());
            var utf8Shard = new ShardKey<int, short>(1, 5678, 9876).ToUtf8();

            //var rom = new ReadOnlyMemory<byte>(shd.ToArray());
            var prms = new ParameterCollection();
            lmbaRead(utf8Shard, prms, new DebugLogger());
            prms.Count.Should().Be(3);
            prms["@ChildShard"].Value.Should().Be(1);
            prms["@ParentRecordId"].Value.Should().Be(5678);
            prms["@ChildRecordId"].Value.Should().Be(9876);
        }

        [Fact]
        public void TestShardWriteLambda()
        {
            var lmbaWrite = OrleansExpressionHelper.BuildShardWriteLambda<TestShardModel>("test", false);
            var lmbaWriteValid = OrleansExpressionHelper.BuildShardWriteLambda<TestShardModel>("test", true);

            var model = new TestShardModel();
            var shardId = lmbaWrite(model.RecordChild.ToArray(), model);
            shardId.Should().Be(model.RecordChild.ShardId);

            var shardId2 = lmbaWriteValid(new ReadOnlyMemory<byte>(model.RecordChild.ToUtf8().ToArray()), model);
            shardId2.Should().Be(model.RecordChild.ShardId);
        }

        [Fact]
        public void TestShardClearLambda()
        {
            var lmbaClear = OrleansExpressionHelper.BuildShardClearLambda<TestShardModel>("test", false, new DebugLogger());
            var lmbaClearValid = OrleansExpressionHelper.BuildShardClearLambda<TestShardModel>("test", true, new DebugLogger());

            var model = new TestShardModel();
            var prms = new ParameterCollection();

            model.RecordChild = new ShardKey<int, short>(1, 456, 432);
            var shardId = lmbaClear(model.RecordChild.ToArray(), model, prms, new DebugLogger());
            shardId.Should().Be(model.RecordChild.ShardId);
            prms.Count.Should().Be(3);
            prms["@ChildShard"].Value.Should().Be(1);
            prms["@ParentRecordId"].Value.Should().Be(456);
            prms["@ChildRecordId"].Value.Should().Be(432);

            var model2 = new TestShardModel();
            var prms2 = new ParameterCollection();
            model2.RecordChild = new ShardKey<int, short>(2, 765, 321);
            var shardId2 = lmbaClear(model2.RecordChild.ToArray(), model2, prms2, new DebugLogger());
            shardId2.Should().Be(model2.RecordChild.ShardId);
            prms2.Count.Should().Be(3);
            prms2["@ChildShard"].Value.Should().Be(2);
            prms2["@ParentRecordId"].Value.Should().Be(765);
            prms2["@ChildRecordId"].Value.Should().Be(321);
        }


        [Fact]
        public void TestSetShardKeyLambdaNoRecordId()
        {
            var shd = new ShardKey<int, short>(3, 123, 4321); // new ShardKey<int, short>('y', 1, 5678, 9876);
            var utf8Shard = shd.ToUtf8();
            var lmbaSetShard = OrleansExpressionHelper.BuildSetShardLambda<TestShardModel2>("test", new DebugLogger());
            var model = new TestShardModel2();
            lmbaSetShard(utf8Shard, model, new DebugLogger());
            model.RecordChild.Should().Be(shd);
        }

        [Fact]
        public void TestShardReadLambdaNoRecordId()
        {
            var lmbaRead = OrleansExpressionHelper.BuildShardReadLambda<TestShardModel2>("test", new DebugLogger());
            var lmbaSetShard = OrleansExpressionHelper.BuildSetShardLambda<TestShardModel2>("test", new DebugLogger());
            var utf8Shard = new ShardKey<int, short>(1, 5678, 9876).ToUtf8();

            //var rom = new ReadOnlyMemory<byte>(shd.ToArray());
            var prms = new ParameterCollection();
            lmbaRead(utf8Shard, prms, new DebugLogger());
            prms.Count.Should().Be(3);
            prms["@ChildShard"].Value.Should().Be(1);
            prms["@ParentRecordId"].Value.Should().Be(5678);
            prms["@ChildRecordId"].Value.Should().Be(9876);
        }

        [Fact]
        public void TestShardWriteLambdaNoRecordId()
        {
            var lmbaWrite = OrleansExpressionHelper.BuildShardWriteLambda<TestShardModel2>("test", false);
            var lmbaWriteValid = OrleansExpressionHelper.BuildShardWriteLambda<TestShardModel2>("test", true);

            var model = new TestShardModel2();
            var shardId = lmbaWrite(model.RecordChild.ToArray(), model);
            shardId.Should().Be(model.RecordChild.ShardId);

            var shardId2 = lmbaWriteValid(new ReadOnlyMemory<byte>(model.RecordChild.ToUtf8().ToArray()), model);
            shardId2.Should().Be(model.RecordChild.ShardId);
        }

        [Fact]
        public void TestShardClearLambdaNoRecordId()
        {
            var lmbaClear = OrleansExpressionHelper.BuildShardClearLambda<TestShardModel2>("test", false, new DebugLogger());
            var lmbaClearValid = OrleansExpressionHelper.BuildShardClearLambda<TestShardModel2>("test", true, new DebugLogger());

            var model = new TestShardModel2();
            var prms = new ParameterCollection();

            model.RecordChild = new ShardKey<int, short>(1, 456, 432);
            var shardId = lmbaClear(model.RecordChild.ToArray(), model, prms, new DebugLogger());
            shardId.Should().Be(model.RecordChild.ShardId);
            prms.Count.Should().Be(3);
            prms["@ChildShard"].Value.Should().Be(1);
            prms["@ParentRecordId"].Value.Should().Be(456);
            prms["@ChildRecordId"].Value.Should().Be(432);

            var model2 = new TestShardModel2();
            var prms2 = new ParameterCollection();
            model2.RecordChild = new ShardKey<int, short>(2, 765, 321);
            var shardId2 = lmbaClear(model2.RecordChild.ToArray(), model2, prms2, new DebugLogger());
            shardId2.Should().Be(model2.RecordChild.ShardId);
            prms2.Count.Should().Be(3);
            prms2["@ChildShard"].Value.Should().Be(2);
            prms2["@ParentRecordId"].Value.Should().Be(765);
            prms2["@ChildRecordId"].Value.Should().Be(321);
        }
    }
}
