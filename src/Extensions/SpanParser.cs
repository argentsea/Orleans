using Orleans.Runtime;
using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.CodeAnalysis.Operations;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;
using Orleans;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace ArgentSea.Orleans
{
    public static class SpanParser
    {
        private const byte cDELIMITER = (byte)'+';
        private const byte cDELIMITER_REPLACE = 0x1D; //using the non-printable UTF8 "groupd separator" value as a delimiter. Unlikely to occur in a normal string, but still a valid, single byte UTF-8 character.


        public static ReadOnlySpan<byte> Extract(this ReadOnlySpan<byte> span, out long result)
        {
            ReadOnlySpan<byte> valueSpan;
            var delimiter = span.IndexOf(cDELIMITER);
            if (delimiter > -1)
            {
                valueSpan = span[..delimiter];
            }
            else
            {
                valueSpan = span;
            }
            if (!Utf8Parser.TryParse(valueSpan, out result, out var len, 'X'))
            {
                throw new InvalidCastException($"Unable to parse key of “{Encoding.UTF8.GetString(valueSpan)}” to a Int64.");
            }
            if (delimiter == -1)
            {
                delimiter = len - 1;
            }
            //return span[slicePosition..];
            return span[(delimiter + 1)..];
        }

        public static ReadOnlySpan<byte> Extract(this ReadOnlySpan<byte> span, out ulong result)
        {
            ReadOnlySpan<byte> valueSpan;
            var delimiter = span.IndexOf(cDELIMITER);
            if (delimiter > -1)
            {
                valueSpan = span[..delimiter];
            }
            else
            {
                valueSpan = span;
            }
            if (!Utf8Parser.TryParse(valueSpan, out result, out var len, 'X') && len == 16)
            {
                throw new InvalidCastException($"Unable to parse key of “{Encoding.UTF8.GetString(valueSpan)}” to a UInt64.");
            }
            if (delimiter == -1)
            {
                delimiter = len - 1;
            }
            return span[(delimiter + 1)..];
        }

        public static ReadOnlySpan<byte> Extract(this ReadOnlySpan<byte> span, out Guid result)
        {
            ReadOnlySpan<byte> valueSpan;
            var delimiter = span.IndexOf(cDELIMITER);
            if (delimiter > -1)
            {
                valueSpan = span[..delimiter];
            }
            else
            {
                valueSpan = span;
            }

            if (!Utf8Parser.TryParse(valueSpan, out result, out var len, 'N') && len == 32)
            {
                throw new InvalidCastException($"Unable to parse key of “{Encoding.UTF8.GetString(valueSpan)}” to a Guid.");
            }
            if (delimiter == -1)
            {
                delimiter = len - 1;
            }
            return span[(delimiter + 1)..];
        }

        public static ReadOnlySpan<byte> Extract(this ReadOnlySpan<byte> span, out int result)
        {
            ReadOnlySpan<byte> valueSpan;
            var delimiter = span.IndexOf(cDELIMITER);
            if (delimiter > -1)
            {
                valueSpan = span[..delimiter];
            }
            else
            {
                valueSpan = span;
            }
            if (!Utf8Parser.TryParse(valueSpan, out result, out var len, 'X') && len == 8)
            {
                throw new InvalidCastException($"Unable to parse key of “{Encoding.UTF8.GetString(valueSpan)}” to a Int32.");
            }
            if (delimiter == -1)
            {
                delimiter = len - 1;
            }
            return span[(delimiter + 1)..];
        }

        public static ReadOnlySpan<byte> Extract(this ReadOnlySpan<byte> span, out uint result)
        {
            ReadOnlySpan<byte> valueSpan;
            var delimiter = span.IndexOf(cDELIMITER);
            if (delimiter > -1)
            {
                valueSpan = span[..delimiter];
            }
            else
            {
                valueSpan = span;
            }
            if (!Utf8Parser.TryParse(valueSpan, out result, out var len, 'X') && len == 8)
            {
                throw new InvalidCastException($"Unable to parse key of “{Encoding.UTF8.GetString(valueSpan)}” to a UInt32.");
            }
            if (delimiter == -1)
            {
                delimiter = len - 1;
            }
            return span[(delimiter + 1)..];
        }

        public static ReadOnlySpan<byte> Extract(this ReadOnlySpan<byte> span, out short result)
        {
            ReadOnlySpan<byte> valueSpan;
            var delimiter = span.IndexOf(cDELIMITER);
            if (delimiter > -1)
            {
                valueSpan = span[..delimiter];
            }
            else
            {
                valueSpan = span;
            }
            if (!Utf8Parser.TryParse(valueSpan, out result, out var len, 'X') && len == 4)
            {
                throw new InvalidCastException($"Unable to parse key of “{Encoding.UTF8.GetString(valueSpan)}” to a Int16.");
            }
            if (delimiter == -1)
            {
                delimiter = len - 1;
            }
            return span[(delimiter + 1)..];
        }

        public static ReadOnlySpan<byte> Extract(this ReadOnlySpan<byte> span, out ushort result)
        {
            ReadOnlySpan<byte> valueSpan;
            var delimiter = span.IndexOf(cDELIMITER);
            if (delimiter > -1)
            {
                valueSpan = span[..delimiter];
            }
            else
            {
                valueSpan = span;
            }
            if (!Utf8Parser.TryParse(valueSpan, out result, out var len, 'X') && len == 4)
            {
                throw new InvalidCastException($"Unable to parse key of “{Encoding.UTF8.GetString(valueSpan)}” to a UInt16.");
            }
            if (delimiter == -1)
            {
                delimiter = len - 1;
            }
            return span[(delimiter + 1)..];
        }

        public static ReadOnlySpan<byte> Extract(this ReadOnlySpan<byte> span, out byte result)
        {
            ReadOnlySpan<byte> valueSpan;
            var delimiter = span.IndexOf(cDELIMITER);
            if (delimiter > -1)
            {
                valueSpan = span[..delimiter];
            }
            else
            {
                valueSpan = span;
            }
            if (!Utf8Parser.TryParse(valueSpan, out result, out var len, 'X') && len == 2)
            {
                throw new InvalidCastException($"Unable to parse key of “{Encoding.UTF8.GetString(valueSpan)}” to a Byte.");
            }
            if (delimiter == -1)
            {
                delimiter = len - 1;
            }
            return span[(delimiter + 1)..];
        }

        public static ReadOnlySpan<byte> Extract(this ReadOnlySpan<byte> span, out sbyte result)
        {
            ReadOnlySpan<byte> valueSpan;
            var delimiter = span.IndexOf(cDELIMITER);
            if (delimiter > -1)
            {
                valueSpan = span[..delimiter];
            }
            else
            {
                valueSpan = span;
            }
            if (!Utf8Parser.TryParse(valueSpan, out result, out var len, 'X') && len == 2)
            {
                throw new InvalidCastException($"Unable to parse key of “{Encoding.UTF8.GetString(valueSpan)}” to a signed byte.");
            }
            if (delimiter == -1)
            {
                delimiter = len - 1;
            }
            return span[(delimiter + 1)..];
        }

        public static ReadOnlySpan<byte> Extract(this ReadOnlySpan<byte> span, out string result)
        {

            ReadOnlySpan<byte> valueSpan;
            var delimiter = span.IndexOf(cDELIMITER);
            if (delimiter > -1)
            {
                valueSpan = span[..delimiter];
            }
            else
            {
                valueSpan = span;
            }
            result = Encoding.UTF8.GetString(valueSpan);
            result.Replace((char)cDELIMITER_REPLACE, (char)cDELIMITER);

            if (delimiter == -1)
            {
                delimiter = result.Length - 1;
            }
            return span[(delimiter + 1)..];
        }

        public static int Append(this Span<byte> buffer, int currentPosition, Guid value)
        {

            Span<byte> buf = stackalloc byte[32];
            if (!Utf8Formatter.TryFormat(value, buf, out var len, 'N'))
            {
                throw new FormatException($"Could not write Guid {value.ToString()} to stream.");
            }
            if (currentPosition > 0)
            {
                buffer[currentPosition] = cDELIMITER;
                currentPosition++;
            }
            buf[..len].CopyTo(buffer.Slice(currentPosition));
            return currentPosition + len;
        }
        public static int Append(this Span<byte> buffer, int currentPosition, long value)
        {

            Span<byte> buf = stackalloc byte[16];
            if (!Utf8Formatter.TryFormat(value, buf, out var len, 'X'))
            {
                throw new FormatException($"Could not write Int64 {value.ToString()} to stream.");
            }
            if (currentPosition > 0)
            {
                buffer[currentPosition] = cDELIMITER;
                currentPosition++;
            }
            buf[..len].CopyTo(buffer.Slice(currentPosition));
            return currentPosition + len;
        }
        public static int Append(this Span<byte> buffer, int currentPosition, ulong value)
        {
            Span<byte> buf = stackalloc byte[16];
            if (!Utf8Formatter.TryFormat(value, buf, out var len, 'X'))
            {
                throw new FormatException($"Could not write UInt64 {value.ToString()} to stream.");
            }
            if (currentPosition > 0)
            {
                buffer[currentPosition] = cDELIMITER;
                currentPosition++;
            }
            buf[..len].CopyTo(buffer.Slice(currentPosition));
            return currentPosition + len;
        }
        public static int Append(this Span<byte> buffer, int currentPosition, int value)
        {
            Span<byte> buf = stackalloc byte[8];
            if (!Utf8Formatter.TryFormat(value, buf, out var len, 'X'))
            {
                throw new FormatException($"Could not write Int32 {value.ToString()} to stream.");
            }
            if (currentPosition > 0)
            {
                buffer[currentPosition] = cDELIMITER;
                currentPosition++;
            }
            buf[..len].CopyTo(buffer.Slice(currentPosition));
            return currentPosition + len;
        }

        public static int Append(this Span<byte> buffer, int currentPosition, uint value)
        {
            Span<byte> buf = stackalloc byte[8];
            if (!Utf8Formatter.TryFormat(value, buf, out var len, 'X'))
            {
                throw new FormatException($"Could not write UInt32 {value.ToString()} to stream.");
            }
            if (currentPosition > 0)
            {
                buffer[currentPosition] = cDELIMITER;
                currentPosition++;
            }
            buf[..len].CopyTo(buffer.Slice(currentPosition));
            return currentPosition + len;
        }

        public static int Append(this Span<byte> buffer, int currentPosition, short value)
        {
            Span<byte> buf = stackalloc byte[4];
            if (!Utf8Formatter.TryFormat(value, buf, out var len, 'X'))
            {
                throw new FormatException($"Could not write Int16 {value.ToString()} to stream.");
            }
            if (currentPosition > 0)
            {
                buffer[currentPosition] = cDELIMITER;
                currentPosition++;
            }
            buf[..len].CopyTo(buffer.Slice(currentPosition));
            return currentPosition + len;
        }

        public static int Append(this Span<byte> buffer, int currentPosition, ushort value)
        {
            Span<byte> buf = stackalloc byte[4];
            if (!Utf8Formatter.TryFormat(value, buf, out var len, 'X'))
            {
                throw new FormatException($"Could not write UInt16 {value.ToString()} to stream.");
            }
            if (currentPosition > 0)
            {
                buffer[currentPosition] = cDELIMITER;
                currentPosition++;
            }
            buf[..len].CopyTo(buffer.Slice(currentPosition));
            return currentPosition + len;
        }

        public static int Append(this Span<byte> buffer, int currentPosition, byte value)
        {
            Span<byte> buf = stackalloc byte[2];
            if (!Utf8Formatter.TryFormat(value, buf, out var len, 'X'))
            {
                throw new FormatException($"Could not write byte {value.ToString()} to stream.");
            }
            if (currentPosition > 0)
            {
                buffer[currentPosition] = cDELIMITER;
                currentPosition++;
            }
            buf[..len].CopyTo(buffer.Slice(currentPosition));
            return currentPosition + len;
        }

        public static int Append(this Span<byte> buffer, int currentPosition, sbyte value)
        {
            Span<byte> buf = stackalloc byte[2];
            if (!Utf8Formatter.TryFormat(value, buf, out var len, 'X'))
            {
                throw new FormatException($"Could not write signed byte {value.ToString()} to stream.");
            }
            if (currentPosition > 0)
            {
                buffer[currentPosition] = cDELIMITER;
                currentPosition++;
            }
            buf[..len].CopyTo(buffer.Slice(currentPosition));
            return currentPosition + len;
        }

        public static int Append(this Span<byte> buffer, int currentPosition, decimal value)
        {
            Span<byte> buf = stackalloc byte[32];
            if (!Utf8Formatter.TryFormat(value, buf, out var len, 'X'))
            {
                throw new FormatException($"Could not write decimal {value.ToString()} to stream.");
            }
            if (currentPosition > 0)
            {
                buffer[currentPosition] = cDELIMITER;
                currentPosition++;
            }
            buf[..len].CopyTo(buffer.Slice(currentPosition));
            return currentPosition + len;
        }

        public static int Append(this Span<byte> buffer, int currentPosition, string value)
        {

            value = value.Replace(((char)cDELIMITER), (char)cDELIMITER_REPLACE);
            var aValue = Encoding.UTF8.GetBytes(value);

            if ((buffer.Length < currentPosition + aValue.Length) || (currentPosition > 0 && buffer.Length < currentPosition))
            {
                    throw new AccessViolationException("Insufficient space in buffer.");
            }
            if (currentPosition > 0)
            {
                buffer[currentPosition] = cDELIMITER;
                currentPosition++;
            }
            aValue.CopyTo(buffer.Slice(currentPosition));
            return currentPosition + aValue.Length;
        }
        public static int Append(this Span<byte> buffer, int currentPosition, ReadOnlySpan<byte> value)
        {
            Span<byte> tmp = stackalloc byte[ value.Length ];
            value.Replace(tmp, cDELIMITER, cDELIMITER_REPLACE);

            if ((buffer.Length < currentPosition + tmp.Length) || (currentPosition > 0 && buffer.Length < currentPosition))
            {
                throw new AccessViolationException("Insufficient space in buffer.");
            }
            if (currentPosition > 0)
            {
                buffer[currentPosition] = cDELIMITER;
                currentPosition++;
            }
            tmp.CopyTo(buffer.Slice(currentPosition));
            return currentPosition + tmp.Length;
        }

        private static readonly ConcurrentDictionary<Type, Lazy<Delegate>> _getIdSpan = new();

        public static IdSpan GetDbGrainId<TModel>(TModel model, ILogger logger)
        {
            if (model is null)
            {
                throw new ArgumentNullException(nameof(model));
            }
            var tModel = typeof(TModel);
            var lazyIdSpanCreator = _getIdSpan.GetOrAdd(tModel, (key) => new Lazy<Delegate>(() => OrleansExpressionHelper.BuildGrainIdFromDbModel<TModel>(logger), LazyThreadSafetyMode.ExecutionAndPublication));
            if (lazyIdSpanCreator.IsValueCreated)
            {
                LoggingExtensions.OrleansDbCacheHit(logger, tModel.Name);
            }
            else
            {
                LoggingExtensions.OrleansDbCacheMiss(logger, tModel.Name);
            }

            return ((Func<TModel, ILogger, IdSpan>)lazyIdSpanCreator.Value)(model, logger);
        }
    }
}
