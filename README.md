# vigg
An implementation of the Bolt protocol, version 4, for communicating with Neo4j from Erlang

Bolt is "vigg" in Swedish, as in Åskvigg (Thunderbolt). 

# Status

This version will connect and communicate with Neo4j using the Bolt version 4 protocol, but only just :)

# Build

    $ rebar3 compile
    
# Test
    
A simple session with Neo4j (found in util:handshake):

    {ok, C} = vigg:connect("localhost", 7687, "neo4j", "neo5j", []),
    
    %
    Requests = vigg:run("RETURN 1 AS num", #{}, #{}),
    Requests2 = vigg:pull(Requests, 1000),
    ?PRINT("Sending ~p~n", [Requests2]),

    %
    Reply = vigg:request(C, Requests2),
    ?PRINT("Got ~p~n", [Reply]),
    
    %
    vigg:disconnect(C).

and the results...

    $ rebar3 shell
    Erlang/OTP 22 [erts-10.5] [source] [64-bit] [smp:20:20] [ds:20:20:10] [async-threads:1] [hipe]
    
    Eshell V10.5  (abort with ^G)
    1> util:handshake().
    vigg_connection(216): Connected to "Neo4j/4.0.4" ("bolt-281")
    util(29): Sending [{run,"RETURN 1 AS num",#{},#{}},{pull,1000}]
    util(33): Got {success,#{fields => ["num"],t_first => [1]}}
    {ok,<0.125.0>}
    

