# Szymon Kubica (sk4520) 12 Feb 2023
defmodule Scout do
  def start(l, acceptors, b) do
    waitfor = acceptors

    self = %{
      leader: l,
      ballot_number: b,
      waitfor: waitfor,
      acceptors: acceptors,
      pvalues: MapSet.new()
    }

    for acceptor <- acceptors do
      send(acceptor, {:p1a, self(), b})
    end

    self |> next
  end

  def next(self) do
    receive do
      {:p1b, a, b, r} ->
        if b == self.ballot_number do
          self =
            self
            |> remove_acceptor_from_waitfor(a)
            |> add_pvalue(r)

          if majority_responded?(self) do
            send(self.leader, {:ADOPTED, self.ballot_number, self.pvalues})
          else
            self |> next
          end
        else
          send(self.leader, {:PREEMPTED, b})
        end
    end
  end

  def majority_responded?(self) do
    length(self.waitfor) < length(self.acceptors) / 2
  end

  def add_pvalue(self, pvalue) do
    %{self | pvalues: MapSet.put(self.pvalues, pvalue)}
  end

  def remove_acceptor_from_waitfor(self, a) do
    %{self | waitfor: List.delete(self.waitfor, a)}
  end
end
