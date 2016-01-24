module cassandra.connection;

import std.stdio;
import vibe.vibe;
import cassandra.client;
import cassandra.frame;
import cassandra.buffer;

final class CassandraFuture
{
    private
    {
        ManualEvent _event;
        CassandraFrame _frame;
    }

    this()
    {
        _event = createManualEvent();
    }

    CassandraFrame result()
    {
        _event.wait();
        return _frame;
    }

    void setResult(CassandraFrame frame)
    {
        _frame = frame;
        _event.emit();
    }
}

final class CassandraConnection
{
    private
    {
        Task _listenTask;
        TCPConnection _conn;
        short _nextStreamId;
        CassandraSettings _settings;
        CassandraFuture[short] _futures;
    }

    this(CassandraSettings settings, int hostIndex)
    {
        _nextStreamId = 0;
        _settings = settings;
        _conn = connectTCP(settings.hosts[hostIndex], settings.port);
        _listenTask = runTask(&listen);
    }

    bool negotiate()
    {
        string[string] map;
        map["CQL_VERSION"] = _settings.cqlVersion;
        auto buffer = new CassandraOutputBuffer();
        buffer.write(map);
        auto request = new CassandraFrame(CassandraOpCode.STARTUP, buffer);
        auto future = send(request);
        auto response = future.result();
        return response.header.opcode == CassandraOpCode.READY;
    }

    void queryAsync()
    {
        
    }

    void query(string query, CassandraConsistency consistency)
    {
        ubyte flags = 0;
        auto buffer = new CassandraOutputBuffer();
        buffer.writeLongString(query);
        buffer.write(cast(short) consistency);
        buffer.write(flags);
        auto request = new CassandraFrame(CassandraOpCode.QUERY, buffer);
        auto future = send(request);
        auto response = future.result();
        writeln(response.header.opcode);
    }

    private short nextStreamId()
    {
        if (_nextStreamId == short.max)
        {
            _nextStreamId = 0;
        }
        else
        {
            _nextStreamId += 1;
        }
        return _nextStreamId;
    }

    private CassandraFuture send(CassandraFrame frame)
    {
        auto streamId = nextStreamId();
        auto future = new CassandraFuture();
        _futures[streamId] = future;
        frame.send(_conn, streamId);
        return future;
    }

    private void listen()
    {
        for (;;)
        {
            auto frame = new CassandraFrame(_conn);
            auto streamId = frame.header.getStreamId();
            auto future = _futures[streamId];
            _futures.remove(streamId);
            future.setResult(frame);
        }
    }

    ~this()
    {
        if(_conn !is null && _conn.connected)
        {
            _conn.close();
        }
        _listenTask.terminate();
    }
}
