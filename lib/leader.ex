# Szymon Kubica (sk4520) 12 Feb 2023
defmodule Leader do
  @compile if Mix.env() == :test, do: :export_all
  def start(config) do
    ballot_num = %BallotNumber{value: 0, leader_index: config.node_num}

    {acceptors, replicas} =
      receive do
        {:BIND, acceptors, replicas} -> {acceptors, replicas}
      end

    self = %{
      type: :leader,
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
    ballot_num = self.ballot_num

    self =
      receive do
        {:PROPOSE, s, c} ->
          self =
            self |> Debug.log("PROPOSE received: command: #{inspect(c)} in slot #{s}", :verbose)

          self =
            if(not exists_proposal_for_slot(self, s)) do
              self =
                self
                |> Debug.log(
                  "No proposals for slot #{s}, adding a proposal: #{inspect({s, c})}",
                  :verbose
                )

              self = self |> add_proposal({s, c})

              self =
                self
                |> Debug.log(
                  "Current proposals: #{MapSet.size(self.proposals)} \n #{inspect(self.proposals)}",
                  :verbose
                )

              self =
                if self.active do
                  spawn(Commander, :start, [
                    self.config,
                    self(),
                    self.acceptors,
                    self.replicas,
                    {self.ballot_num, s, c}
                  ])

                  send(self.config.monitor, {:COMMANDER_SPAWNED, self.config.node_num})

                  self
                  |> Debug.log("Commander spawned: command: #{inspect(c)} in slot #{s}", :success)
                else
                  self
                end

              self
            else
              self
            end

          self

        {:ADOPTED, ^ballot_num, pvalues} ->
          self =
            self
            |> Debug.log("ADOPTED received: ballot: #{inspect(ballot_num)}", :success)
            |> Debug.log("Proposals before update #{inspect(self.proposals)}", :verbose)
            |> Debug.log("Pvalues #{inspect(pvalues)}", :verbose)
            |> Debug.log("Pmax #{inspect(pmax(pvalues))}", :verbose)
            |> update_proposals(pmax(pvalues))
            |> Debug.log(
              "#{MapSet.size(self.proposals)} Proposals after update #{inspect(self.proposals)}",
              :verbose
            )

          for {s, c} <- self.proposals do
            spawn(Commander, :start, [
              self.config,
              self(),
              self.acceptors,
              self.replicas,
              {self.ballot_num, s, c}
            ])

            send(self.config.monitor, {:COMMANDER_SPAWNED, self.config.node_num})

            self
            |> Debug.log("Commander spawned: command: #{inspect(c)} in slot #{s}", :success)
          end

          self
          |> activate
          |> next

        {:PREEMPTED, %BallotNumber{value: value} = b} ->
          self = self |> Debug.log("Received PREEMPTED message for ballot #{inspect(b)}", :error)
          Process.sleep(Enum.random(1..100))

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

  defp add_proposal(self, proposal) do
    %{self | proposals: MapSet.put(self.proposals, proposal)}
  end

  defp exists_proposal_for_slot(self, slot_number) do
    proposals = for {^slot_number, _c} = proposal <- self.proposals, do: proposal
    length(proposals) > 0
  end

  defp activate(self) do
    %{self | active: true}
  end

  defp deactivate(self) do
    %{self | active: false}
  end

  defp update_ballot_number(self, preempting_number) do
    %{
      self
      | ballot_num: %BallotNumber{self.ballot_num | value: preempting_number + 1}
    }
  end

  defp update_proposals(self, max_pvals) do
    remaining_proposals =
      for {s, _c} = proposal <- self.proposals,
          not update_exists?(s, max_pvals),
          into: MapSet.new(),
          do: proposal

    %{self | proposals: MapSet.union(max_pvals, remaining_proposals)}
  end

  defp update_exists?(slot_number, pvalues) do
    updates = for {^slot_number, _c} = pvalue <- pvalues, do: pvalue
    length(updates) != 0
  end

  defp pmax(pvalues) do
    for {b, s, c} <- pvalues,
        Enum.all?(
          for {b1, ^s, _c1} <- pvalues,
              do: BallotNumber.compare(b1, b) == :lt or BallotNumber.compare(b1, b) == :eq
        ),
        into: MapSet.new(),
        do: {s, c}
  end
end
