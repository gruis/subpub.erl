-module(subpub).
-export([listen/0, listen/1, bcastmsgs/1, publisher/2, reader/2]).

-define(TCP_OPTIONS, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]).

listen() ->
  listen(6389).

listen(Port) ->
  {ok, LSocket} = gen_tcp:listen(Port, ?TCP_OPTIONS),
  register(broadcaster, spawn(?MODULE, bcastmsgs, [[]])),
  accept(LSocket).

%% Wait for a  message from any client and broadcast
%% it to all other client handlers. The client handlers
%% decide if their client is subscribed to the message
%% topic or not.
bcastmsgs(Connections) ->
  receive
    {publish, Topic, Msg} ->
      [C ! {publish, Topic, Msg} || C <- Connections],
      bcastmsgs(Connections);
    {register, Proc} ->
      bcastmsgs([Proc|Connections]);
    Other ->
      io:format("received unrecognized message: ~p.~n", [Other]),
      bcastmsgs(Connections)
  end.

%% Accept a connection from a client.
%% When a client does connect, start a publisher 
%% and a reader for that client.
accept(LSocket) ->
    {ok, Socket} = gen_tcp:accept(LSocket),
    Publisher    = spawn(?MODULE, publisher, [init, Socket]),
    Reader       = spawn(?MODULE, reader, [Socket, Publisher]),
    Publisher   ! {register, reader, Reader},
    broadcaster ! {register, Publisher},
    accept(LSocket).


% publisher sends data to client and manages its description
publisher(init, Client) ->
  % TODO link to Writer
  put(subs, []),
  put(psubs, []),
  publisher(Client).

publisher(Client) ->
  receive
    {register, reader, Reader} ->
      put(reader, Reader);
    {publish, Topic, Msg} -> 
      case lists:any(fun(E) -> E == Topic end, get(subs)) of
          true -> tell_client(Client, publish(Client, Topic, Msg));
          _ -> nothing
      end;
    {subscribe, Topic} ->
      put(subs, [Topic | get(subs)]),
      get(reader) ! { subs_cnt, length(get(subs))};
    {unsubscribe, Topic} ->
      Subs = get(subs),
      put(subs, [E || E <- Subs, E =/= Topic]),
      get(reader) ! { subs_cnt, length(get(subs)) };
    {subs} ->
      get(reader) ! { subs, get(subs) };
    {quit} ->
      io:format("received quit message, but not doing anything");
    M -> 
      io:format("(70) got an unrecognized message ~p~n", [M])
  end,
  publisher(Client).


%% reader listens to the client for data
reader(Client, Publisher) ->
  put(publisher, Publisher),
  reader(Client).

reader(Client) ->
    case gen_tcp:recv(Client, 0) of
        {ok, Data} ->
          [{command, Command} | Rest] = redis_cmd_parser:parse(binary_to_list(Data)),
          Reply = procmd(Client, Command, Rest),
          tell_client(Client, Reply),
          reader(Client);
        {error, closed} ->
          ok;
        {error, Reason} ->
          io:format("failed: ~p.~n", [Reason])
    end.



procmd(_Client, "PUBLISH", [{topic, Topic}, {msg, Msg}]) ->
  broadcaster ! {publish, Topic, Msg},
  ":0";

procmd(Client, "SUBSCRIBE", [{topics, Topics}]) ->
  lists:foldl(
    fun(Topic, Reply) -> string:concat(Reply, subscribe(Client, Topic)) end, 
    "", 
    Topics);

procmd(Client, "UNSUBSCRIBE", [{topics, ""}]) ->
  get(publisher) ! { subs },
  receive
    {subs, Topics} -> true
  end,
  lists:foldl(
    fun(Topic, Reply) -> string:concat(Reply, unsubscribe(Client, Topic)) end, 
    "", 
    Topics);

procmd(Client, "UNSUBSCRIBE", [{topics, Topics}]) ->
  lists:foldl(
    fun(Topic, Reply) -> string:concat(Reply, unsubscribe(Client, Topic)) end, 
    "", 
    Topics);

procmd(Client, "QUIT", _) ->
  tell_client(Client, "+ok"),
  exit("QUIT");

procmd(Client, "SHUTDOWN", _) ->
  tell_client(Client, "+ok"),
  halt();

procmd(_Client, Command, _Args) ->
  string:concat("unrecognized command ", Command).


subscribe(_Client, Topic) ->
  get(publisher) ! {subscribe, Topic},
  receive
    {subs_cnt, Cnt} ->
      lists:foldl(fun(E, Sum) -> string:concat(Sum, E) end, "*3\r\n$9\r\nsubscribe\r\n$", 
        [ integer_to_list(length(Topic)),
          "\r\n",
          Topic,
          "\r\n:",
          integer_to_list(Cnt),
          "\r\n"
        ]);
    M -> io:format("got an unrecognized message ~p~n", [M])

  end.

unsubscribe(_Client, Topic) ->
  get(publisher) ! {unsubscribe, Topic},
  receive
    {subs_cnt, Cnt} ->
      lists:foldl(fun(E, Sum) -> string:concat(Sum, E) end, "*3\r\n$11\r\nunsubscribe\r\n$", 
        [ integer_to_list(length(Topic)),
          "\r\n",
          Topic,
          "\r\n:",
          integer_to_list(Cnt),
          "\r\n"
        ]);
    M -> io:format("(166) unrecognized message ~p", [M])
  end.

publish(_Client, Topic, Msg) ->
  lists:foldl(fun(E, Sum) -> string:concat(Sum, E) end, "*3\r\n$7\r\nmessage\r\n$", 
    [ integer_to_list(length(Topic)),
      "\r\n",
      Topic,
      "\r\n$",
      integer_to_list(length(Msg)),
      "\r\n",
      Msg,
      "\r\n"
    ]).


tell_client(Client, {Msg, "\r\n"}) ->
  gen_tcp:send(Client, list_to_binary(string:concat(Msg, "\r\n")));

tell_client(Client, {Msg, Tail}) ->
  gen_tcp:send(Client, list_to_binary(string:concat(string:concat(Msg, Tail), "\r\n")));

tell_client(_Client, []) ->
  % code smell
  fail;

tell_client(Client, Msg) ->
  tell_client(Client, lists:split(length(Msg) -2, Msg)).
