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
  end

  defp next(self) do
    self =
      receive do
        {:REQUEST, c} ->
          self |> add_request(c)

        {:DECISION, s, c} ->
          # Process the decision
          self = self |> add_decision({s, c})
          self |> process_pending_decicions
      end

    self |> propose
  end

  defp propose(self) do
    if self.slot_in < self.slot_out + self.config.window_size and MapSet.size(self.requests) > 0 do
    end
  end

  defp perform(self, {client, cid, op} = command) do
    if earlier_slot_filled?(self, command) or isreconfig?(op) do
      self |> increment_slot_out
    else
      # Apply the change to the state
      self = self |> increment_slot_out
      send(client, {:CLIENT_RESPONSE, cid, result})
      self
    end
  end

  defp earlier_slot_filled?(self, {client, cid, op}) do
    slots_filled = for {s, {^client, ^cid, ^op}} <- decisions, s < self.slot_out, do: s
    length(slots_filled) != 0
  end

  # TODO: implement
  defp isreconfig?(op) do
    false
  end

  defp process_pending_decicions(self) do
    decicions_for_current_slot_out =
      MapSet.filter(self.decisions, fn {s, c} -> s == self.slot_out end)

    self =
      if MapSet.size(decisions_for_current_slot_out) != 0 do
        {self.slot_out, c} = MapSet.to_list(decisions_for_current_slot_out)[0]

        self =
          for {self.slot_out, c2} = proposal <- self.proposals do
            self
            |> remove_proposal(proposal)
            |> if c != c2, do: self |> add_request(c2), else: self
          end

        self
        |> perform(c)
        |> process_pending_decisions
      else
        self
      end
  end

  defp increment_slot_out(self) do
    %{self | slot_out: self.slot_out + 1}
  end

  defp remove_proposal(self, proposal) do
    %{self | proposals: MapSet.delete(self.proposals, proposal)}
  end

  defp add_request(self, request) do
    %{self | requests: MapSet.put(self.requests, request)}
  end

  defp add_decision(self, decision) do
    %{self | decisions: MapSet.put(self.decisions, decision)}
  end
end
