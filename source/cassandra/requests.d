module cassandra.requests;

import std.stdio;
import std.string;
import vibe.vibe;
import cassandra.frame;
import cassandra.buffer;

bool negotiateConnection(TCPConnection conn, string cqlVersion)
{
    return true;
}
