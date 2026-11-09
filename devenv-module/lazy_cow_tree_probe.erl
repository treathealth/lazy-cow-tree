%% Started in every BEAM of the devenv shell and lazy-cow-tree's services
%% (`-s lazy_cow_tree_probe` in ERL_AFLAGS: before the node's own `-s`/`-eval`, which may never return): answers on
%% <state>/beam/<os pid>.sock with how busy the node is, one line of key=value, so
%% removing a worktree stops what nobody uses and spares what's working:
%%
%%   util      scheduler utilization over the last minute, 0..1
%%   clients   connections to this node's listening TCP ports now
%%   idle_s    seconds since it last had clients or used over 2% of its schedulers
%%   shell     1 with an interactive shell (iex, erl) on a terminal
%%   uptime_s
%%
%% Never fails the node: anything going wrong only stops the probe.
-module(lazy_cow_tree_probe).
-export([start/0]).

-define(TICK_MS, 5000).
-define(WINDOW, 12).
-define(BUSY_UTIL, 0.02).

start() ->
    case dir() of
        false -> ok;
        Dir -> spawn(fun() -> catch run(Dir) end), ok
    end.

dir() ->
    case os:getenv("LAZY_COW_TREE_HOME") of
        Home when Home =/= false, Home =/= "" -> filename:join(Home, "beam");
        _ ->
            case os:getenv("HOME") of
                false -> false;
                H -> filename:join([H, ".local", "state", "lazy-cow-tree", "beam"])
            end
    end.

run(Dir) ->
    ok = filelib:ensure_path(Dir),
    Path = filename:join(Dir, os:getpid() ++ ".sock"),
    _ = file:delete(Path),
    {ok, Listen} = gen_tcp:listen(0, [binary, {ifaddr, {local, Path}}, {active, false}]),
    %% Kept on for as long as this process lives.
    erlang:system_flag(scheduler_wall_time, true),
    Self = self(),
    Acceptor = spawn_link(fun() -> accept(Listen, Self) end),
    State = #{sample => sample(), utils => [], idle_since => now_s(), started => now_s()},
    erlang:send_after(?TICK_MS, self(), tick),
    loop(State, Acceptor).

loop(#{sample := Prev, utils := Utils, idle_since := IdleSince} = S, Acceptor) ->
    receive
        tick ->
            Sample = sample(),
            Util = util(Prev, Sample),
            Utils1 = lists:sublist([Util | Utils], ?WINDOW),
            Busy = Util > ?BUSY_UTIL orelse clients() > 0,
            erlang:send_after(?TICK_MS, self(), tick),
            loop(S#{sample := Sample, utils := Utils1,
                    idle_since := case Busy of true -> now_s(); false -> IdleSince end},
                 Acceptor);
        {report, From} ->
            From ! {report, report(S)},
            loop(S, Acceptor)
    end.

accept(Listen, Probe) ->
    {ok, Sock} = gen_tcp:accept(Listen),
    Probe ! {report, self()},
    receive {report, Line} -> catch gen_tcp:send(Sock, Line) after 1000 -> ok end,
    gen_tcp:close(Sock),
    accept(Listen, Probe).

report(#{utils := Utils, idle_since := IdleSince, started := Started}) ->
    Util = case Utils of [] -> 0.0; _ -> lists:sum(Utils) / length(Utils) end,
    Clients = clients(),
    Idle = case Clients > 0 of true -> 0; false -> now_s() - IdleSince end,
    io_lib:format("util=~.3f clients=~b idle_s=~b shell=~b uptime_s=~b~n",
                  [Util, Clients, Idle, shell(), now_s() - Started]).

sample() ->
    lists:sort(erlang:statistics(scheduler_wall_time)).

%% Normal schedulers only: dirty ones are counted while blocked in long NIF calls.
util(Prev, Now) ->
    N = erlang:system_info(schedulers),
    Pairs = [{A1 - A0, T1 - T0}
             || {{I, A0, T0}, {I, A1, T1}} <- lists:zip(Prev, Now), I =< N],
    case lists:sum([T || {_, T} <- Pairs]) of
        0 -> 0.0;
        Total -> lists:sum([A || {A, _} <- Pairs]) / Total
    end.

%% Connections accepted on one of this node's listening TCP ports (not unix sockets,
%% this probe's own included).
clients() ->
    Socks = [P || P <- erlang:ports(), erlang:port_info(P, name) =:= {name, "tcp_inet"}],
    Ports = [{inet:sockname(P), inet:peername(P)} || P <- Socks],
    Listening = [Port || {{ok, {_, Port}}, {error, _}} <- Ports, is_integer(Port)],
    length([Port || {{ok, {_, Port}}, {ok, _}} <- Ports, is_integer(Port),
                    lists:member(Port, Listening)]).

%% An interactive shell on a terminal: someone may be typing in it.
shell() ->
    case whereis(user_drv) =/= undefined andalso lists:keyfind(terminal, 1, io:getopts(user)) of
        {terminal, true} -> 1;
        _ -> 0
    end.

now_s() ->
    erlang:monotonic_time(second).
