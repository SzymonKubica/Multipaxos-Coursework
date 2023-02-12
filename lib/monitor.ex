
# distributed algorithms, n.dulay 31 jan 2023
# coursework, paxos made moderately complex

defmodule Monitor do

# _______________________________________________________________________ Setters
def clock(self, v) do Map.put(self, :clock, v) end

def seen(self, k, v) do Map.put(self, :seen, Map.put(self.seen, k, v)) end

def done(self, k, v) do Map.put(self, :done,  Map.put(self.done, k, v)) end

def log(self, k, v) do Map.put(self, :log,  Map.put(self.log, k, v)) end

def commanders_spawned(self, k, v) do
  Map.put(self, :commanders_spawned,  Map.put(self.commanders_spawned, k, v))
end

def commanders_finished(self, k, v) do
  Map.put(self, :commanders_finished,  Map.put(self.commanders_finished, k, v))
end

def scouts_spawned(self, k, v) do
  Map.put(self, :scouts_spawned, Map.put(self.scouts_spawned, k, v))
end

def scouts_finished(self, k, v) do
  Map.put(self, :scouts_finished, Map.put(self.scouts_finished, k, v))
end

# __________________________________________________________________________

def start(config) do
  self = %{
    config: config,  clock: 0,  seen: Map.new,  done: Map.new, log: Map.new,

    scouts_spawned:      Map.new,  scouts_finished:     Map.new,
    commanders_spawned:  Map.new,  commanders_finished: Map.new,
  }
  self |> start_print_timeout()
       |> next()
end # start

def next(self) do
  receive do
  { :DB_MOVE, db, seqnum, transaction } ->
    { :MOVE, amount, from, to } = transaction
    done = Map.get(self.done, db, 0)
    expecting = done + 1

    if seqnum != expecting do
      Helper.node_halt "  ** error db #{db}: seq #{seqnum} expecting #{expecting}"
    end # if

    self = case Map.get(self.log, seqnum) do
      nil ->
        self |> log(seqnum, %{amount: amount, from: from, to: to})

       t -> # already logged - check transaction against logged value (t)
        if amount != t.amount or from != t.from or to != t.to do
  	      Helper.node_halt "Monitor:  ** error db #{db}.#{done} [#{amount},#{from},#{to}] "
            <>
          "= log #{done}/#{map_size(self.log)} [#{t.amount},#{t.from},#{t.to}]"
        end # if
        self
    end # case

    self |> done(db, seqnum)
         |> next()

  { :CLIENT_REQUEST, server_num } ->  # client requests seen by replicas
    value = Map.get(self.seen, server_num, 0)
    self |> seen(server_num, value + 1)
         |> next()

  { :SCOUT_SPAWNED, server_num } ->
    value = Map.get(self.scouts_spawned, server_num, 0)
    self |> scouts_spawned(server_num, value + 1)
         |> next()

  { :SCOUT_FINISHED, server_num } ->
    value = Map.get(self.scouts_finished, server_num, 0)
    self |> scouts_finished(server_num, value + 1)
         |> next()

  { :COMMANDER_SPAWNED, server_num } ->
    value = Map.get(self.commanders_spawned, server_num, 0)
    self |> commanders_spawned(server_num, value + 1)
         |> next()

  { :COMMANDER_FINISHED, server_num } ->
    value = Map.get(self.commanders_finished, server_num, 0)
    self |> commanders_finished(server_num, value + 1)
         |> next()

    { :PRINT } ->
    clock  = self.clock + self.config.print_after
    self   = self |> clock(clock)

    sorted = self.seen |> Map.to_list |> List.keysort(0)
    IO.puts "time = #{clock} client requests seen = #{inspect sorted}"
    sorted = self.done |> Map.to_list |> List.keysort(0)
    IO.puts "time = #{clock}     db requests done = #{inspect sorted}"

    if self.config.debug_level > 0 do
      sorted = self.scouts_spawned  |> Map.to_list |> List.keysort(0)
      IO.puts "time = #{clock}            scouts up = #{inspect sorted}"
      sorted = self.scouts_finished |> Map.to_list |> List.keysort(0)
      IO.puts "time = #{clock}          scouts down = #{inspect sorted}"

      sorted = self.commanders_spawned  |> Map.to_list |> List.keysort(0)
      IO.puts "time = #{clock}        commanders up = #{inspect sorted}"
      sorted = self.commanders_finished |> Map.to_list |> List.keysort(0)
      IO.puts "time = #{clock}      commanders down = #{inspect sorted}"
    end # if

    IO.puts ""
    self |> start_print_timeout()
         |> next()

  # ** ADD ADDITIONAL MESSAGES OF YOUR OWN HERE

  unexpected ->
    Helper.node_halt "monitor: unexpected message #{inspect unexpected}"
  end # receive
end # next

def start_print_timeout(self) do
  Process.send_after(self(), { :PRINT }, self.config.print_after)
  self
end # start_print_timeout

end # Monitor
