import List
# Szymon Kubica (sk4520) 12 Feb 2023
defmodule Replica do
  def start(config, database) do
    leaders =
      receive do
        {:BIND, leaders} -> leaders
      end

    self = %{
      config: config,
      leaders: leaders,
      database: database,
      slot_in: 1,
      slot_out: 1,
      requests: MapSet.new(),
      proposals: MapSet.new(),
      decisions: MapSet.new()
    }

    self |> next
  end

  defp next(self) do
    self =
      receive do
        {:CLIENT_REQUEST, c} ->
          self |> log("Received client request to perform #{inspect(c)}")
          send(self.config.monitor, {:CLIENT_REQUEST, self.config.node_num})
          self |> add_request(c)

        {:DECISION, s, c} ->
          self |> log("Received a decision to perform #{inspect(c)} in slot #{s}")

          self = self |> add_decision({s, c})
          self = self |> process_pending_decisions
          self

        unexpected ->
          IO.puts("Replica: unexpected message #{inspect(unexpected)}")
          self
      after
        1000 ->
          self
      end

    self |> propose
  end

  defp propose(self) do
    if self.slot_in < self.slot_out + self.config.window_size and MapSet.size(self.requests) > 0 do
      self = self |> try_to_reconfigure

      self =
        if slot_in_not_decided?(self) do
          c = get_next_request(self)
          self |> log("Proposing new request: #{inspect(c)} in slot #{self.slot_in}")

          self = self |> remove_request(c)
          self = self |> add_proposal({self.slot_in, c})

          for l <- self.leaders do
            send(l, {:PROPOSE, self.slot_in, c})
          end

          self
        else
          self
        end

      self = self |> increment_slot_in
      self |> log("Current slot in: #{self.slot_in}")
      self |> propose
    else
      self |> next
    end
  end

  defp get_next_request(self) do
    first(MapSet.to_list(self.requests))
  end

  defp slot_in_not_decided?(self) do
    slot_in = self.slot_in
    decisions_for_slot_in = for {^slot_in, _c} = decision <- self.decisions, do: decision
    length(decisions_for_slot_in) == 0
  end

  defp try_to_reconfigure(self) do
    reconfiguration_slot = self.slot_in - self.config.window_size

    decisions_for_reconfig_slot =
      for {^reconfiguration_slot, {_client, _cid, command}} <- self.decisions,
          do: command

    if length(decisions_for_reconfig_slot) > 0 and isreconfig?(first(decisions_for_reconfig_slot)) do
      self |> update_leaders(first(decisions_for_reconfig_slot).leaders)
    else
      self
    end
  end

  defp perform(self, {client, cid, op} = command) do
    self |> log("Trying to perform #{inspect(command)} in slot #{self.slot_out}")

    self =
      if already_processed?(self, command) or isreconfig?(op) do
        self
        |> log(
          "Command: #{inspect(command)} was already processed \n current slot out: #{self.slot_out}"
        )

        self = self |> increment_slot_out
        self
      else
        send(self.database, {:EXECUTE, op})

        self
        |> log(
          "Command: #{inspect(command)} in slot #{self.slot_out} successfully written to the database."
        )

        send(client, {:CLIENT_RESPONSE, cid, command})
        self = self |> increment_slot_out
        self
      end

    self
  end

  defp already_processed?(self, {client, cid, op}) do
    slots_filled =
      for {s, {^client, ^cid, ^op}} = decision <- self.decisions, s < self.slot_out, do: decision

    if length(slots_filled) != 0,
      do:
        self
        |> log("Slots filled: #{inspect(slots_filled)} \n current slot out: #{self.slot_out}")

    length(slots_filled) != 0
  end

  defp isreconfig?(op) do
    case op do
      %{leaders: _leaders} -> true
      _ -> false
    end
  end

  defp process_pending_decisions(self) do
    slot_out = self.slot_out

    decisions_for_current_slot_out =
      for {^slot_out, _c} = decision <- self.decisions, into: MapSet.new(), do: decision

    self =
      if MapSet.size(decisions_for_current_slot_out) > 0 do
        {^slot_out, c} = first(MapSet.to_list(decisions_for_current_slot_out))

        proposals_for_slot_out = for {^slot_out, _c} = proposal <- self.proposals, do: proposal

        # There can only be one proposal for slot out
        self =
          if length(proposals_for_slot_out) > 0 do
            {slot_out, c2} = first(proposals_for_slot_out)
            self = self |> remove_proposal({slot_out, c2})
            self = if c != c2, do: self |> add_request(c2), else: self

            self
            |> log("Comparing commands: \n #{inspect(c)} \n #{inspect(c2)} \n result: #{c != c2}")

            self
          else
            self
          end

        self = self |> perform(c)
        self = self |> process_pending_decisions

        self
      else
        self
      end

    self
  end

  defp update_leaders(self, leaders) do
    %{self | leaders: leaders}
  end

  defp increment_slot_out(self) do
    %{self | slot_out: self.slot_out + 1}
  end

  defp increment_slot_in(self) do
    %{self | slot_in: self.slot_in + 1}
  end

  defp add_proposal(self, proposal) do
    %{self | proposals: MapSet.put(self.proposals, proposal)}
  end

  defp remove_proposal(self, proposal) do
    %{self | proposals: MapSet.delete(self.proposals, proposal)}
  end

  defp add_request(self, request) do
    %{self | requests: MapSet.put(self.requests, request)}
  end

  defp remove_request(self, request) do
    %{self | requests: MapSet.delete(self.requests, request)}
  end

  defp add_decision(self, decision) do
    %{self | decisions: MapSet.put(self.decisions, decision)}
  end

  defp log(self, message) do
    DebugLogger.log(
      self.config,
      :replica,
      "Replica at #{self.config.node_name}",
      message
    )
  end
end
