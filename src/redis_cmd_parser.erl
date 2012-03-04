-module(redis_cmd_parser).

-export([parse/1]).


parse(Input) ->
  case next_line(Input) of
    {ok, Line, Rest} -> parse(Line, Rest);
    null -> null
  end.


%% Unified Request Protocol parser

% Start of a new message
% match *Nargs
parse([42 | Nargs], Rest) ->
  [Command | Args] = parse_args(list_to_integer(string:strip(Nargs)), Rest),
  parse({command, Command}, {args, Args});

% match $Bytes
parse([36 | Argl], Input) ->
  Bytes         = list_to_integer(string:strip(Argl)),
  {Value, _Rest} = lists:split(Bytes, Input),
  {"\r\n", Rest} = lists:split(2, _Rest),
  {ok, Value, Rest};

% "*3\r\n$7\r\nPUBLISH\r\n$6\r\nquotes\r\n$75\r\nWhen Chuck Norris wants a steak, cows volunteer. It's just easier that way.\r\n"
parse({command, "PUBLISH"}, {args, [Topic | Msg]}) ->
  case length(Msg) of
    1 -> [Pmsg|_] = Msg;
    _ -> Pmsg = string:join(Msg, " ")
  end,
  [{command, "PUBLISH"}, {topic, Topic}, {msg, Pmsg}];

% "*3\r\n$9\r\nSUBSCRIBE\r\n$6\r\nquotes\r\n$6\r\nscores\r\n"
parse({command, "SUBSCRIBE"}, {args, Topics}) ->
  [{command, "SUBSCRIBE"}, {topics, Topics}];
parse({command, "UNSUBSCRIBE"}, {args, Topics}) ->
  [{command, "UNSUBSCRIBE"}, {topics, Topics}];

parse({command, "PSUBSCRIBE"}, {args, Topics}) ->
  [{command, "PSUBSCRIBE"}, {patterns, Topics}];
parse({command, "PUNSUBSCRIBE"}, {args, Topics}) ->
  [{command, "PUNSUBSCRIBE"}, {patterns, Topics}];

% "*1\r\n$4\r\nQUIT\r\n"
parse({command, "QUIT"}, {args, _}) ->
  [{command, "QUIT"}];
parse({command, "SHUTDOWN"}, {args, _}) ->
  [{command, "SHUTDOWN"}];

%% Legacy single line parser

parse(Line, Rest) when is_list(Line), is_list(Rest) ->
  {ok, Command, Args} = next_token(Line),
  parse({command, Command}, {args, string:tokens(Args, " ")}).


parse_args(Num, Input) ->
  case Num of
      0 ->
        [];
      _ ->
        case next_line(Input) of
          {ok, Line, _Rest} -> 
            {ok, Value, Rest} = parse(Line, _Rest),
            [Value | parse_args(Num - 1, Rest)];
          null -> ""
        end
  end.

next_line(Input) ->
  case string:str(Input, "\r\n") of 
    0 -> case string:len(Input) of 
          0 -> null;
          _Len -> {ok, Input, ""}
        end;
    Eofl ->
      {Line, Rest} = lists:split(Eofl + 1, Input),
      {Trim, "\r\n"} = lists:split(Eofl - 1, Line),
      {ok, Trim, Rest}
  end.

next_token(Line) ->
  case string:str(Line, " ") of
    0 -> case string:len(Line) of
          0 -> null;
          _Len -> {ok, Line, ""}
        end;
    Eow -> 
      {Word, Rest} = lists:split(Eow, Line),
      {Trim, " "} = lists:split(Eow - 1, Word),
      {ok, Trim, Rest}
  end.
