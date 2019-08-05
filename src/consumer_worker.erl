-module(consumer_worker).
-behaviour(gen_server).

%% API.
-export([start_link/1]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
    filters,
    time_window,
    batch_size,
    destination,
    index
}).

%% API.

-spec start_link(#{}) -> {ok, pid()}.
start_link(Config) ->
    gen_server:start_link(?MODULE, [Config], []).

%% gen_server.

init([Config]) ->
    Filters = maps:get(filters, Config),
    TimeWindow = maps:get(time_window, Config),
    BatchSize = maps:get(batch_size, Config),
    Destination = maps:get(destination, Config),
    Index = data_reader:get_index(),
    {ok, #state{filters = Filters,
                time_window = TimeWindow,
                batch_size = BatchSize,
                destination = Destination,
                index = Index}}.

handle_call(_, _From, State) ->
    {reply, ignored, State}.

handle_cast(_, State) ->
    {noreply, State}.
    
handle_info(timeout, #state{filters = Filters,
                            batch_size = BatchSize,
                            time_window = TimeWindow,
                            destination = Destination,
                            index = Index} = State) ->
    TimeBefore = erlang:system_time(microsecond),
    TimeToWait = TimeBefore + TimeWindow,
    {Data, NewIndex} = data_reader:filter_data_upto(Filters, BatchSize, Index),
    data_sender:send_data(Data, Destination),
    data_reader:confirm(NewIndex),
    State1 = State#state{index = NewIndex},
    TimeAfter = erlang:system_time(microsecond),
    case TimeToWait - TimeAfter of
        LTZ when LTZ =< 0 ->
            {noreply, State1, 0};
        TimeLeft ->
            case TimeLeft > 1000 of
                true ->
                    {noreply, State1, TimeLeft div 1000};
                false ->
                    %% Busy wait
                    busy_wait(TimeToWait),
                    {noreply, State1, 0}
            end
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

busy_wait(TimeToWait) ->
    erlang:yield(),
    TimeAfter = erlang:system_time(microsecond),
    case TimeAfter > TimeToWait of
        true -> ok;
        false -> busy_wait(TimeToWait)
    end.
