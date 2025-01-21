using FluentAssertions;

namespace ArgentSea.Orleans.Test
{
    public class GrainIdSerializationTests
    {
        [Fact]
        public void TestGuid()
        {
            var position = 0;
            var spn = new Span<byte>(new byte[32]);
            var guidTest = Guid.NewGuid();
            position = SpanParser.Append(spn, position, guidTest);

            SpanParser.Extract(spn, out Guid result);
            result.Should().Be(guidTest);
        }
        [Fact]
        public void TestGuids()
        {
            var position = 0;
            var spn = new Span<byte>(new byte[65]);
            var guidTest1 = Guid.NewGuid();
            var guidTest2 = Guid.NewGuid();
            position = SpanParser.Append(spn, position, guidTest1);
            position = SpanParser.Append(spn, position, guidTest2);

            ReadOnlySpan<byte>  roSpan = spn;
            roSpan.Extract(out Guid result1).Extract(out Guid result2);
            result1.Should().Be(guidTest1);
            result2.Should().Be(guidTest2);
        }
        [Fact]
        public void TestLong()
        {
            var position = 0;
            var spn = new Span<byte>(new byte[16]);
            var lngTest = 12345L;
            position = SpanParser.Append(spn, position, lngTest);

            SpanParser.Extract(spn, out long result);
            result.Should().Be(lngTest);
        }
        [Fact]
        public void TestLongs()
        {
            var position = 0;
            var spn = new Span<byte>(new byte[33]);
            var lngTest1 = long.MaxValue;
            var lngTest2 = long.MinValue;
            position = SpanParser.Append(spn, position, lngTest1);
            position = SpanParser.Append(spn, position, lngTest2);

            ReadOnlySpan<byte> roSpan = spn;
            roSpan.Extract(out long result1).Extract(out long result2);
            result1.Should().Be(lngTest1);
            result2.Should().Be(lngTest2);
        }
        [Fact]
        public void TestLots()
        {
            var position = 0;
            var spn = new Span<byte>(new byte[75]);
            var lngTest = long.MaxValue;
            var guidTest = Guid.NewGuid();
            var intTest = 0;
            var strTest = "Wanna know";
            var srtTest = (short)12345;
            position = SpanParser.Append(spn, position, lngTest);  // 16 + 1
            position = SpanParser.Append(spn, position, guidTest); // 32 + 1
            position = SpanParser.Append(spn, position, intTest);  // 8 + 1
            position = SpanParser.Append(spn, position, strTest);  // 10 + 1
            position = SpanParser.Append(spn, position, srtTest);  // 4

            ReadOnlySpan<byte> roSpan = spn;
            roSpan = roSpan.Extract(out long lngResult);
            roSpan = roSpan.Extract(out Guid guidResult);
            roSpan = roSpan.Extract(out int intResult);
            roSpan = roSpan.Extract(out string strResult);
            roSpan = roSpan.Extract(out short srtResult);
            lngResult.Should().Be(lngTest);
            guidResult.Should().Be(guidTest);
            intResult.Should().Be(intTest);
            strResult.Should().Be(strTest);
            srtResult.Should().Be(srtTest);
        }
    }
}