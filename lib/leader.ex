# Szymon Kubica (sk4520) 12 Feb 2023
defmodule Leader do
  def start(config) do
    ballot_num = %BallotNumber{value: 0, leader_index: config.node_num}

    {acceptors, replicas} =
      receive do
        {:BIND, acceptors, replicas} -> {acceptors, replicas}
      end

    self = %{
      config: config,
      ballot_num: ballot_num,
      acceptors: acceptors,
      replicas: replicas,
      active: false,
      proposals: MapSet.new()
    }

    spawn(Scout, :start, [self.config, self(), acceptors, ballot_num])
    send(self.config.monitor, {:SCOUT_SPAWNED, self.config.node_num})

    self |> next
  end

  def next(self) do
    self =
      receive do
        {:PROPOSE, s, c} ->
          self |> log("Proposal received: slot_number: #{inspect(s)}, command: #{inspect(c)}")

          self =
            if(not exists_proposal_for_slot(self, s)) do
              self |> log("No proposals for slot #{s} found, adding a proposal...")
              self = self |> add_proposal({s, c})
              self |> log("#{inspect(self.proposals)}")

              if self.active do
                spawn(Commander, :start, [
                  self.config,
                  self(),
                  self.acceptors,
                  self.replicas,
                  {self.ballot_num, s, c}
                ])

                send(self.config.monitor, {:COMMANDER_SPAWNED, self.config.node_num})

                self |> log("Commander spawned for slot: #{s} and command: #{inspect(c)}")
              end

              self
            else
              self
            end

          self

        {:ADOPTED, b, pvalues} ->
          self |> log("Received ADOPTED message for ballot #{inspect(b)}")
          self = self |> update_proposals(pvalues)

          for {s, c} <- self.proposals do
            spawn(Commander, :start, [
              self.config,
              self(),
              self.acceptors,
              self.replicas,
              {self.ballot_num, s, c}
            ])

            send(self.config.monitor, {:COMMANDER_SPAWNED, self.config.node_num})
          end

          self
          |> activate
          |> next

        {:PREEMPTED, %BallotNumber{value: value} = b} ->
          self |> log("Received PREEMPTED message for ballot #{inspect(b)}")

          self =
            case BallotNumber.compare(b, self.ballot_num) do
              :gt ->
                self =
                  self
                  |> deactivate
                  |> update_ballot_number(value)

                spawn(Scout, :start, [self.config, self(), self.acceptors, self.ballot_num])
                send(self.config.monitor, {:SCOUT_SPAWNED, self.config.node_num})
                self

              _ ->
                self
            end

          self
      end

    self |> next
  end

  def add_proposal(self, proposal) do
    %{self | proposals: MapSet.put(self.proposals, proposal)}
  end

  def exists_proposal_for_slot(self, slot_number) do
    MapSet.size(MapSet.filter(self.proposals, fn {s, _c} -> s == slot_number end)) > 0
  end

  def activate(self) do
    %{self | active: true}
  end

  def deactivate(self) do
    %{self | active: false}
  end

  def update_ballot_number(self, preempting_number) do
    %{
      self
      | ballot_num: %BallotNumber{self.ballot_num | value: preempting_number + 1}
    }
  end

  def update_proposals(self, pvalues) do
    max_proposals = pmax(pvalues)

    remaining_proposals =
      for {s, _c} = proposal <- self.proposals,
          not update_exists?(s, max_proposals),
          into: MapSet.new(),
          do: proposal

    %{self | proposals: MapSet.union(max_proposals, remaining_proposals)}
  end

  def update_exists?(slot_number, pvalues) do
    updates = for {^slot_number, _c} = pvalue <- pvalues, do: pvalue
    length(updates) != 0
  end

  def pmax(pvalues) do
    for {b, s, c} <- pvalues, into: MapSet.new() do
      if Enum.all?(
           for {b1, ^s, _c1} <- pvalues,
               do: BallotNumber.compare(b1, b) == :lt or BallotNumber.compare(b1, b) == :eq
         ) do
        {s, c}
      end
    end
  end

  defp log(self, message) do
    DebugLogger.log(
      self.config,
      :leader,
      "Leader#{self.config.node_num} at #{self.config.node_name}",
      message
    )
  end
end
