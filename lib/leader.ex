# Szymon Kubica (sk4520) 12 Feb 2023
defmodule Leader do
  def start(leader_index) do
    ballot_num = %BallotNumber{value: 0, leader_index: leader_index}

    {acceptors, replicas} =
      receive do
        {:BIND, acceptors, replicas} -> {acceptors, replicas}
      end

    self = %{
      ballot_num: ballot_num,
      acceptors: acceptors,
      replicas: replicas,
      active: false,
      proposals: MapSet.new()
    }

    self |> next
  end

  def next(self) do
    self =
      receive do
        {:PROPOSE, s, c} ->
          if not exists_proposal_for_slot(self, s) do
            self = self |> add_proposal({s, c})

            if self.active do
              spawn(Commander, :start, [
                self(),
                self.acceptors,
                self.replicas,
                {self.ballot_num, s, c}
              ])
            end

            self
          end

        {:ADOPTED, _b, pvalues} ->
          self = self |> update_proposals(pvalues)

          for {s, c} <- self.proposals do
            spawn(Commander, :start, [
              self(),
              self.acceptors,
              self.replicas,
              {self.ballot_num, s, c}
            ])
          end

          self
          |> activate
          |> next

        {:PREEMPTED, {value, _leader} = b} ->
          if b > self.ballot_num do
            self =
              self
              |> deactivate
              |> update_ballot_number(value)

            spawn(Scout, :start, [self(), self.acceptors, self.ballot_number])
            self
          else
            self
          end
      end

    self |> next
  end

  def add_proposal(self, proposal) do
    %{self | proposals: MapSet.put(self.proposals, proposal)}
  end

  def exists_proposal_for_slot(self, slot_number) do
    MapSet.size(MapSet.filter(self.proposals, fn {s, _c} -> s == slot_number end)) == 0
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
      | ballot_number: %BallotNumber{self.ballot_number | value: preempting_number + 1}
    }
  end

  def update_proposals(self, pvalues) do
    max_proposals = pmax(pvalues)

    remaining_proposals =
      MapSet.filter(self.proposals, fn {s, _c} -> update_exists?(s, max_proposals) end)

    %{self | proposals: MapSet.union(max_proposals, remaining_proposals)}
  end

  def update_exists?(slot_number, pvalues) do
    updates =
      for {s, _c} <- pvalues do
        if s == slot_number, do: s
      end

    length(updates) != 0
  end

  def pmax(pvalues) do
    # As defined in the paper, pmax needs to return the pairs {s, c} such that
    # for each slot s we associate it with the command corresponding to the
    # highest ballot number. We can do it by first getting the set of all slots
    slots = for {_b, s, _c} <- pvalues, do: s

    # Now we can iterate over all slots and pick the command corresponding to
    # the hightest ballot number.

    for s <- slots, into: MapSet.new() do
      pvals_for_s = MapSet.filter(pvalues, fn {_b, s2, _c} -> s2 == s end)
      {_b, s, c} = Enum.max(pvals_for_s)
      {s, c}
    end
  end
end
