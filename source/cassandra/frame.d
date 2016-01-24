module cassandra.frame;;

import vibe.vibe;
import std.bitmanip;
import std.conv;
import cassandra.buffer;

enum CassandraOpCode : ubyte
{
    ERROR = 0x00,
    STARTUP = 0x01,
    READY = 0x02,
    AUTHENTICATE = 0x03,
    OPTIONS = 0x05,
    SUPPORTED = 0x06,
    QUERY = 0x07,
    RESULT = 0x08,
    PREPARE = 0x09,
    EXECUTE = 0x0A,
    REGISTER = 0x0B,
    EVENT = 0x0C,
    BATCH = 0x0D,
    AUTH_CHALLENGE = 0x0E,
    AUTH_RESPONSE = 0x0F,
    AUTH_SUCCESS = 0x10
}

private struct CassandraFrameHeader
{
    align(1) mixin(bitfields!(
        uint, "protover", 7,
        bool, "response", 1));

    align(1) mixin(bitfields!(
        bool, "compression", 1,
        bool, "tracing", 1,
        uint, "",        6));

    align(1) ubyte[2] stream;
    align(1) CassandraOpCode opcode;
    align(1) ubyte[4] length;

    short getStreamId() 
    {
        return bigEndianToNative!short(stream);
    }

    void setStreamId(short id)
    {
        stream = nativeToBigEndian!short(id);
    }

    int getContentLength()
    {
        return bigEndianToNative!int(length);
    }

    void setContentLength(int len)
    {
        length = nativeToBigEndian!int(len);
    }
}

final class CassandraFrame
{
    CassandraFrameHeader header;
    ubyte[] _content;

    this(CassandraOpCode opcode, CassandraOutputBuffer buf)
    {
        header = CassandraFrameHeader();
        header.protover = 3;
        header.response = false;
        header.compression = false;
        header.tracing = false;
        header.opcode = opcode;

        _content = buf.toBytes();
        header.setContentLength(cast(short) _content.length);
    }

    this(TCPConnection conn)
    {
        conn.read((cast(ubyte*)&header)[0 .. CassandraFrameHeader.sizeof]);
        _content = new ubyte[header.getContentLength];
        conn.read(_content);
    }

    void send(TCPConnection conn, short streamId)
    {
        header.setStreamId(streamId);
        conn.write((cast(ubyte*)&header)[0 .. CassandraFrameHeader.sizeof]);
        conn.write(_content);
    }
}
