# vigg
An implementation of the Bolt protocol, version 4, for communicating with Neo4j from Erlang

Bolt is "vigg" in Swedish, as in Åskvigg (Thunderbolt). 

# Status

This version will connect and communicate with Neo4j using the Bolt version 4 protocol, but only just :)

# Build

    $ rebar3 compile
    
# Test
    
A simple session with Neo4j:

    {ok, C} = vigg:connect("localhost", 7687, "neo4j", "neo5j", []),
    
    % Composing primitives, part of the Bolt protocol
    Stmt = vigg:run("RETURN 1 AS num", #{}, #{}),
    Pull = vigg:pull(1000),

    % Sending the whole lot the server, picking up the results
    Reply = vigg:request(C, Stmt ++ Pull),
    lists:map(fun(E) -> io:fwrite("~p~n", [E]) end, Reply),
    
    %
    vigg:disconnect(C).

and an example, bulk-loading a file with CQL statements:

    $ rebar3 shell
    Erlang/OTP 22 [erts-10.5] [source] [64-bit] [smp:20:20] [ds:20:20:10] [async-threads:1] [hipe]
    
    Eshell V10.5  (abort with ^G)
    1> C = util:connect().
    vigg_connection(276): Connected to "Neo4j/4.0.4" ("bolt-342")
    <0.138.0>
    2> util:bulk_load_whole(C, "./stmts.cql").
    ok   
    
