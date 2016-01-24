module cassandra.buffer;

import std.string;
import std.bitmanip;
import std.outbuffer;

enum CassandraConsistency : short {
    ANY = 0x0000,
    ONE = 0x0001,
    TWO = 0x0002,
    THREE = 0x0003,
    QUORUM = 0x004,
    ALL = 0x0005,
    LOCAL_QUORUM = 0x0006,
    EACH_QUORUM = 0x0007,
    SERIAL = 0x0008,
    LOCAL_SERIAL = 0x0009,
    LOCAL_ONE = 0x000A,
}

final class CassandraOutputBuffer : OutBuffer
{
    alias write = OutBuffer.write;

    this()
    {
        super();
    }

    override void write(short s)
    {
        super.write(nativeToBigEndian!short(s));
    }

    override void write(int i)
    {
        super.write(nativeToBigEndian!int(i));
    }

    override void write(long l)
    {
        super.write(nativeToBigEndian!long(l));
    }

    void write(string s)
    {
        write(cast(short) s.length);
        super.write(representation(s));
    }

    void writeLongString(string s)
    {
        write(cast(int) s.length);
        super.write(representation(s));
    }

    void write(string[] list)
    {
        write(cast(short) list.length);
        foreach (s; list)
        {
            write(s);
        }
    }

    void write(ubyte[] bytes)
    {
        write(cast(int) bytes.length);
        super.write(bytes);
    }

    void writeShortBytes(ubyte[] bytes)
    {
        write(cast(short) bytes.length);
        super.write(bytes);
    }

    void write(string[string] map)
    {
        auto keys = map.keys;
        write(cast(short) keys.length);
        foreach (key; keys)
        {
            write(key);
            write(map[key]);
        }
    }

    void write(string[][string] map)
    {
        auto keys = map.keys;
        write(cast(short) keys.length);
        foreach (key; keys)
        {
            write(key);
            write(map[key]);
        }
    }
}
