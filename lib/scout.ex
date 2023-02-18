# Szymon Kubica (sk4520) 12 Feb 2023
defmodule Scout do
  def start(config, l, acceptors, b) do
    waitfor = acceptors

    self = %{
      type: :scout,
      config: config,
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
        self =
          self
          |> Debug.log(
            "Phase_1_b message received:\n" <>
              "--> Acceptor: #{inspect(a)}\n" <>
              "--> Ballot number: #{inspect(b)}\n" <>
              "--> Pvalue: #{inspect(r)}.",
            :verbose
          )

        if BallotNumber.compare(b, self.ballot_number) == :eq do
          self =
            self
            |> remove_acceptor_from_waitfor(a)
            |> add_pvalues(r)

          if majority_responded?(self) do
            send(self.leader, {:ADOPTED, self.ballot_number, self.pvalues})
            send(self.config.monitor, {:SCOUT_FINISHED, self.config.node_num})
          else
            self |> next
          end
        else
          send(self.leader, {:PREEMPTED, b})
          send(self.config.monitor, {:SCOUT_FINISHED, self.config.node_num})
        end
    end
  end

  def majority_responded?(self) do
    length(self.waitfor) < div(length(self.acceptors) + 1, 2)
  end

  def add_pvalues(self, pvalues) do
    %{self | pvalues: MapSet.union(self.pvalues, pvalues)}
  end

  def remove_acceptor_from_waitfor(self, a) do
    %{self | waitfor: List.delete(self.waitfor, a)}
  end
end
