# Modified by Szymon Kubica (sk4520) 18 Feb 2023
defmodule Leader do
  @compile if Mix.env() == :test, do: :export_all

  # ____________________________________________________________________ Setters

  defp add_proposal(self, proposal) do
    %{self | proposals: MapSet.put(self.proposals, proposal)}
  end

  defp activate(self) do
    %{self | active: true}
  end

  defp deactivate(self) do
    %{self | active: false}
  end

  defp update_ballot_number(self, new_value) do
    %{
      self
      | ballot_num: %BallotNumber{self.ballot_num | value: new_value + 1, timeout: self.timeout}
    }
  end

  defp increase_timeout(self) do
    %{
      self
      | timeout: round(self.timeout * self.config.leader_timeout_increase_factor)
    }
  end

  defp decrease_timeout(self) do
    new_ballot_timeout =
      max(0, self.ballot_num.timeout - self.config.leader_timeout_decrease_const)

    new_timeout =
      max(
        self.config.min_leader_timeout,
        new_ballot_timeout
      )

    %{
      self
      | timeout: new_timeout,
        ballot_num: %BallotNumber{self.ballot_num | timeout: new_ballot_timeout}
    }
  end

  # ____________________________________________________________________________

  def start(config) do
    {acceptors, replicas} =
      receive do
        {:BIND, acceptors, replicas} -> {acceptors, replicas}
      end

    self = %{
      type: :leader,
      config: config,
      ballot_num: %BallotNumber{
        value: 0,
        leader_index: config.node_num,
        leader_pid: self(),
        timeout: config.leader_starting_timeout
      },
      timeout: config.leader_starting_timeout,
      acceptors: acceptors,
      replicas: replicas,
      active: false,
      proposals: MapSet.new()
    }

    self
    |> spawn_scout
    |> next
  end

  def next(self) do
    receive do
      {:RESPONSE_REQUESTED, requestor, ballot} ->
        if self.active and BallotNumber.equal?(ballot, self.ballot_num) do
          send(requestor, {:STILL_ALIVE, self.ballot_num})
        end

        self
        |> Monitor.notify(:PING_RESPONSE_SENT)
        |> next

      {:PROPOSAL_CHOSEN} ->
        self
        |> decrease_timeout
        |> Monitor.notify(:TIMEOUT_DECREASED, self.timeout)
        |> next

      {:PROPOSE, s, c} ->
        self = self |> Debug.log("PROPOSE received: command: #{inspect(c)} in slot #{s}")

        if self |> exists_proposal_for?(s), do: self |> next

        proposal = {s, c}

        self =
          self
          |> Debug.log("Slot #{s} empty, adding a proposal: #{inspect({s, c})}")
          |> add_proposal(proposal)
          |> Debug.log("Proposals: #{MapSet.size(self.proposals)} \n #{inspect(self.proposals)}")

        if not self.active, do: self |> next

        self
        |> spawn_commander(proposal)
        |> Debug.log("Commander spawned for: #{inspect(c)} in slot #{s}", :success)
        |> next

      {:ADOPTED, b, pvalues} ->
        # We cannot just pin the value of self.ballot_num in the match because
        # we might have updated the value of the timeout in the meantime in which case the ballot will be different.
        if not BallotNumber.equal?(b, self.ballot_num), do: self |> next

        self =
          self
          |> Debug.log("ADOPTED received: ballot: #{inspect(b)}", :success)
          |> Debug.log(
            "Proposals before update #{inspect(self.proposals)}\n" <>
              "--> Pvalues: #{inspect(pvalues)}\n" <>
              "--> Pmax: #{inspect(pmax(pvalues))}"
          )
          |> update_proposals(pmax(pvalues))
          |> Debug.log("Proposals after update #{inspect(self.proposals)}")

        commander_spawning_logs =
          for {s, c} = proposal <- self.proposals, into: [] do
            self |> spawn_commander(proposal)
            "Commander spawned: command: #{inspect(c)} in slot #{s}"
          end

        self
        |> Debug.log(Enum.join(commander_spawning_logs, "\n--> "))
        |> activate
        |> next

      {:PREEMPTED, b} ->
        self = self |> Debug.log("Received PREEMPTED message for ballot #{inspect(b)}", :error)

        if BallotNumber.less_or_equal?(b, self.ballot_num), do: self |> next
        Process.send_after(self(), {:TIMEOUT_REACHED}, b.timeout)

        self
        |> ping_preempting_leader(b)
    end
  end

  defp ping_preempting_leader(self, %BallotNumber{value: value} = b) do
    preempting_leader = b.leader_pid
    send(preempting_leader, {:RESPONSE_REQUESTED, self(), b})
    self |> Monitor.notify(:PING_SENT)

    receive do
      {:TIMEOUT_REACHED} ->
        self
        |> deactivate
        |> increase_timeout
        |> Monitor.notify(:TIMEOUT_INCREASED, self.timeout)
        |> update_ballot_number(value)
        |> spawn_scout
        |> next
    after
      0 ->
        receive do
          {:STILL_ALIVE, current_ballot} ->
            # Here we invoke the ping with the currenb ballot received from the leader
            # that way the timeout will be updated
            # change so that the ping interval is configureable

            self
            |> ping_preempting_leader(current_ballot)
        after
          b.timeout ->
            self
            |> deactivate
            |> increase_timeout
            |> Monitor.notify(:TIMEOUT_INCREASED, self.timeout)
            |> update_ballot_number(value)
            |> spawn_scout
            |> next
        end
    end
  end

  defp spawn_commander(self, {s, c} = _proposal) do
    spawn(Commander, :start, [
      self.config,
      self(),
      self.acceptors,
      self.replicas,
      %Pvalue{ballot_num: self.ballot_num, slot_num: s, command: c}
    ])

    self |> Monitor.notify(:COMMANDER_SPAWNED)
  end

  defp spawn_scout(self) do
    spawn(Scout, :start, [self.config, self(), self.acceptors, self.ballot_num])
    self |> Monitor.notify(:SCOUT_SPAWNED)
  end

  defp exists_proposal_for?(self, slot_number) do
    proposals = for {^slot_number, _c} = proposal <- self.proposals, do: proposal
    length(proposals) > 0
  end

  defp update_proposals(self, max_pvals) do
    remaining_proposals =
      for {s, _c} = proposal <- self.proposals,
          not update_exists?(s, max_pvals),
          into: MapSet.new(),
          do: proposal

    %{self | proposals: MapSet.union(max_pvals, remaining_proposals)}
  end

  defp update_exists?(slot_number, proposals) do
    updates = for {^slot_number, _c} = proposal <- proposals, do: proposal
    length(updates) != 0
  end

  defp pmax(pvalues) do
    for %Pvalue{ballot_num: b, slot_num: s, command: c} <- pvalues,
        Enum.all?(
          for %Pvalue{ballot_num: b1, slot_num: ^s} <- pvalues,
              do: BallotNumber.less_or_equal?(b1, b)
        ),
        into: MapSet.new(),
        do: {s, c}
  end
end
