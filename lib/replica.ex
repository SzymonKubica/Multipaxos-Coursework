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
        {:REQUEST, c} ->
          self |> add_request(c)

        {:DECISION, s, c} ->
          # Process the decision
          self = self |> add_decision({s, c})
          self |> process_pending_decisions
      end

    self |> propose
  end

  defp propose(self) do
    if self.slot_in < self.slot_out + self.config.window_size and MapSet.size(self.requests) > 0 do
      self = self |> try_to_reconfigure

      self =
        if slot_in_not_decided?(self) do
          c = get_next_request(self)

          self =
            self
            |> remove_request(c)
            |> add_proposal({self.slot_in, c})

          for l <- self.leaders do
            send(l, {:PROPOSE, self.slot_in, c})
          end

          self
        end

      self
      |> increment_slot_in
      |> propose
    else
      self
    end
  end

  defp get_next_request(self) do
    MapSet.to_list(self.requests)[0]
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

    if length(decisions_for_reconfig_slot) > 0 and isreconfig?(decisions_for_reconfig_slot[0]) do
      self |> update_leaders(decisions_for_reconfig_slot[0].leaders)
    else
      self
    end
  end

  defp perform(self, {client, cid, op} = command) do
    if earlier_slot_filled?(self, command) or isreconfig?(op) do
      self |> increment_slot_out
    else
      # Apply the change to the state
      send(self.database, {:EXECUTE, command})
      self = self |> increment_slot_out
      send(client, {:CLIENT_RESPONSE, cid, command})
      self
    end
  end

  defp earlier_slot_filled?(self, {client, cid, op}) do
    slots_filled = for {s, {^client, ^cid, ^op}} <- self.decisions, s < self.slot_out, do: s
    length(slots_filled) != 0
  end

  defp isreconfig?(op) do
    case op do
      %{leaders: _leaders} -> true
      _ -> false
    end
  end

  defp process_pending_decisions(self) do
    decisions_for_current_slot_out =
      MapSet.filter(self.decisions, fn {s, _c} -> s == self.slot_out end)

    if MapSet.size(decisions_for_current_slot_out) != 0 do
      slot_out = self.slot_out
      {^slot_out, c} = MapSet.to_list(decisions_for_current_slot_out)[0]

      proposals_for_slot_out =
        for {^slot_out, _c} = proposal <- self.proposals do
          proposal
        end

      # There can only be one proposal for slot out
      self =
        if length(proposals_for_slot_out) > 0 do
          {_s, c2} = proposal = proposals_for_slot_out[0]
          self = self |> remove_proposal(proposal)
          if c != c2, do: self |> add_request(c2), else: self
        end

      self
      |> perform(c)
      |> process_pending_decisions
    else
      self
    end
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
end
