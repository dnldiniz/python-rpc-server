# python-rpc-server
Asyncio-based Python RPC Server implemented as state machine, supporting TLS connections based on signed certificates. Implements both RPC Server and Client mode, and listens to both "southbound" and "northbound" connections. Full unit test suite included.

# Requirements
To be able to use the TLS authenticated connection, a root and a signed certificate are needed.
These need to be placed at /sbin/rpcsd/
For more information on how to generate those, please contact me.

# Basic functionality
While on server mode, the daemon listens to the interface configured in config.py.
As it is, the first expected message from a client is an "inform" RPC, which should give the server some basic information about the client.
If the inform message is accepted, the server can decide to queue more RPC's to be communicated while it remains on "RPC Server" mode.

# Client mode
After finishing its role as "RPC Server", by proceding through all the defined states, it is possible to implement a "RPC Client" mode.
In this mode, the client can send requests itself, which would take effect on the server side.
