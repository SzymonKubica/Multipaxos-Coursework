# Szymon Kubica (sk4520) 12 Feb 2023
defmodule Scout do
  def start(config, l, acceptors, b) do
    waitfor = acceptors

    self = %{
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
        self
        |> log(
          "p1b message received from acceptor: #{inspect(a)}\n" <>
            "--> Ballot number: #{inspect(b)}\n" <>
            "--> Pvalue: #{inspect(r)}."
        )

        if BallotNumber.compare(b, self.ballot_number) == :eq do
          self = self |> remove_acceptor_from_waitfor(a)
          self = self |> add_pvalue(r)

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

  def add_pvalue(self, pvalue) do
    %{self | pvalues: MapSet.put(self.pvalues, pvalue)}
  end

  def remove_acceptor_from_waitfor(self, a) do
    %{self | waitfor: List.delete(self.waitfor, a)}
  end

  defp log(self, message) do
    DebugLogger.log(
      self.config,
      :scout,
      "Scout at #{self.config.node_name}",
      message
    )
  end
end
