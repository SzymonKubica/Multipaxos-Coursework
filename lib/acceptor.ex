# Szymon Kubica (sk4520) 12 Feb 2023
defmodule Acceptor do
  def start(config) do
    ballot_num = BallotNumber.bottom()
    accepted = MapSet.new()

    self = %{
      type: :acceptor,
      id_line: "Acceptor#{config.node_num}",
      config: config,
      ballot_num: ballot_num,
      accepted: accepted
    }

    self |> next
  end

  def next(self) do
    self =
      receive do
        {:p1a, l, b} ->
          self |> Debug.log("Phase 1 a received: ballot: #{inspect(b)}", :verbose)

          self =
            case BallotNumber.compare(b, self.ballot_num) do
              :gt -> self |> update_ballot_num(b)
              _ -> self
            end

          self
          |> Debug.log("Sending p1b response for ballot: #{inspect(self.ballot_num)}", :verbose)

          send(l, {:p1b, self(), self.ballot_num, self.accepted})
          self

        {:p2a, l, {b, _s, _c} = pvalue} ->
          self =
            case BallotNumber.compare(b, self.ballot_num) do
              :eq -> self |> add_to_accepted(pvalue)
              _ -> self
            end

          send(l, {:p2b, self(), self.ballot_num})
          self

        unexpected ->
          IO.puts("Acceptor: unexpected message #{inspect(unexpected)}")
          self
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
