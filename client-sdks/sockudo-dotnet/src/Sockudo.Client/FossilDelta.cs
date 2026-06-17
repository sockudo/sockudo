namespace Sockudo.Client;

public static class FossilDelta
{
    private const int HashWidth = 16;
    private const string Digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~";
    private static readonly Dictionary<byte, int> Values = Digits
        .Select((value, index) => new KeyValuePair<byte, int>((byte)value, index))
        .ToDictionary(entry => entry.Key, entry => entry.Value);

    private sealed class Reader
    {
        private readonly byte[] _data;
        public int Position { get; set; }

        public Reader(byte[] data)
        {
            _data = data;
        }

        public bool HasBytes => Position < _data.Length;

        public byte ReadByte()
        {
            if (Position >= _data.Length)
            {
                throw new DeltaFailure("out of bounds");
            }
            return _data[Position++];
        }

        public char ReadCharacter() => (char)ReadByte();

        public int ReadInteger()
        {
            var value = 0;
            while (HasBytes)
            {
                var raw = ReadByte();
                if (!Values.TryGetValue(raw, out var mapped))
                {
                    Position -= 1;
                    break;
                }
                value = (value << 6) + mapped;
            }
            return value;
        }
    }

    public static byte[] Apply(byte[] @base, byte[] delta)
    {
        var reader = new Reader(delta);
        var outputSize = reader.ReadInteger();
        if (reader.ReadCharacter() != '\n')
        {
            throw new DeltaFailure("size integer not terminated by newline");
        }

        var output = new List<byte>(outputSize);
        var total = 0;
        while (reader.HasBytes)
        {
            var count = reader.ReadInteger();
            switch (reader.ReadCharacter())
            {
                case '@':
                    {
                        var offset = reader.ReadInteger();
                        if (reader.HasBytes && reader.ReadCharacter() != ',')
                        {
                            throw new DeltaFailure("copy command not terminated by comma");
                        }
                        total += count;
                        if (total > outputSize)
                        {
                            throw new DeltaFailure("copy exceeds output file size");
                        }
                        if (offset + count > @base.Length)
                        {
                            throw new DeltaFailure("copy extends past end of input");
                        }
                        output.AddRange(@base.Skip(offset).Take(count));
                        break;
                    }
                case ':':
                    {
                        total += count;
                        if (total > outputSize)
                        {
                            throw new DeltaFailure("insert command gives an output larger than predicted");
                        }
                        if (reader.Position + count > delta.Length)
                        {
                            throw new DeltaFailure("insert count exceeds size of delta");
                        }
                        output.AddRange(delta.Skip(reader.Position).Take(count));
                        reader.Position += count;
                        break;
                    }
                case ';':
                    {
                        var bytes = output.ToArray();
                        if (count != Checksum(bytes))
                        {
                            throw new DeltaFailure("bad checksum");
                        }
                        if (total != outputSize)
                        {
                            throw new DeltaFailure("generated size does not match predicted size");
                        }
                        return bytes;
                    }
                default:
                    throw new DeltaFailure("unknown delta operator");
            }
        }
        throw new DeltaFailure("unterminated delta");
    }

    private static int Checksum(byte[] data)
    {
        var sum0 = 0;
        var sum1 = 0;
        var sum2 = 0;
        var sum3 = 0;
        var index = 0;
        var remaining = data.Length;

        while (remaining >= HashWidth)
        {
            sum0 += data[index + 0] + data[index + 4] + data[index + 8] + data[index + 12];
            sum1 += data[index + 1] + data[index + 5] + data[index + 9] + data[index + 13];
            sum2 += data[index + 2] + data[index + 6] + data[index + 10] + data[index + 14];
            sum3 += data[index + 3] + data[index + 7] + data[index + 11] + data[index + 15];
            index += HashWidth;
            remaining -= HashWidth;
        }

        while (remaining >= 4)
        {
            sum0 += data[index + 0];
            sum1 += data[index + 1];
            sum2 += data[index + 2];
            sum3 += data[index + 3];
            index += 4;
            remaining -= 4;
        }

        sum3 += (sum2 << 8) + (sum1 << 16) + (sum0 << 24);
        switch (remaining)
        {
            case 3:
                sum3 += data[index + 2] << 8;
                sum3 += data[index + 1] << 16;
                sum3 += data[index + 0] << 24;
                break;
            case 2:
                sum3 += data[index + 1] << 16;
                sum3 += data[index + 0] << 24;
                break;
            case 1:
                sum3 += data[index + 0] << 24;
                break;
        }

        return sum3;
    }
}

public sealed class DeltaFailure : SockudoException
{
    public DeltaFailure(string message) : base(message)
    {
    }
}
