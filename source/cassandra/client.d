module cassandra.client;

import vibe.vibe;
import std.stdio;
import cassandra.connection;

final class CassandraSettings
{
    short port;
    uint poolSize;
    string[] hosts;
    string keyspace;
    string cqlVersion;

    this()
    {
        port = 9042;
        poolSize = 4;
        cqlVersion = "3.2.0";
    }
}

final class CassandraClient
{
    private
    {
        CassandraSettings _settings;
        CassandraConnection[] _pool;
    }

    this(CassandraSettings settings)
    {
        _settings = settings;
        _pool = new CassandraConnection[settings.poolSize];
    }

}
