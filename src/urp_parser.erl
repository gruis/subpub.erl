-module(urp_parser).

-export([parse/1]).


parse([Prefix | Nargs], Rest) when Prefix == "*" ->
    soemthing;

parse(Line, Rest) when is_list(Line), is_list(Rest) ->
  {ok, Command, Args} = next_token(Line),
  parse({command, Command}, {args, Args});

parse({command, "PUBLISH"}, {args, Args}) ->
  {ok, Topic, Msg} = next_token(Args),
  [{command, "PUBLISH"}, {topic, Topic}, {msg, Msg}].

parse(Input) ->
  case next_line(Input) of
    {ok, Line, Rest} -> parse(Line, Rest);
    null -> null
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
