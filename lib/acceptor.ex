# Szymon Kubica (sk4520) 12 Feb 2023
defmodule Acceptor do
  def start() do
    ballot_num = BallotNumber.bottom()
    accepted = MapSet.new()
    self = %{ballot_num: ballot_num, accepted: accepted}
    self |> next
  end

  def next(self) do
    receive do
      {:p1a, l, b} ->
        self = if b > self.ballot_num, do: self |> update_ballot_num(b), else: self
        send(l, {:p1b, self(), self.ballot_num, self.accepted})

      {:p2a, l, {b, _s, _c} = pvalue} ->
        self = if b == self.ballot_num, do: self |> add_to_accepted(pvalue), else: self
        send(l, {:p2b, self(), self.ballot_num})

      unexpected ->
        IO.puts("Acceptor: unexpected message #{inspect(unexpected)}")
    end

    self |> next
  end

  def update_ballot_num(self, ballot_num) do
    %{self | ballot_num: ballot_num}
  end

  def add_to_accepted(self, pvalue) do
    %{self | accepted: MapSet.put(self.accepted, pvalue)}
  end
end
