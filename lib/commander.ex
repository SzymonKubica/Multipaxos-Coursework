# Szymon Kubica (sk4520) 12 Feb 2023
defmodule Commander do
  # ____________________________________________________________________ Setters

  defp remove_acceptor_from_waitfor(self, a) do
    %{self | waitfor: List.delete(self.waitfor, a)}
  end

  # ____________________________________________________________________________

  def start(config, l, acceptors, replicas, pvalue) do
    self = %{
      type: :commander,
      config: config,
      leader: l,
      waitfor: acceptors,
      acceptors: acceptors,
      replicas: replicas,
      pvalue: pvalue
    }

    for acceptor <- acceptors do
      send(acceptor, {:p2a, self(), pvalue})
    end

    self |> next
  end

  def next(self) do
    receive do
      {:p2b, a, b} ->
        self = self |> Debug.log("Phase_2_b message received: ballot: #{inspect(b)}", :verbose)
        {ballot_num, s, c} = self.pvalue

        if not BallotNumber.equal?(b, ballot_num) do
          send(self.leader, {:PREEMPTED, b})
          self |> commander_finished
        end

        self = self |> remove_acceptor_from_waitfor(a)

        if not majority_accepted?(self), do: self |> next

        self |> Debug.log("Majority achieved for: #{inspect(self.pvalue)}", :success)

        for replica <- self.replicas, do: send(replica, {:DECISION, s, c})

        self |> commander_finished
    end
  end

  defp majority_accepted?(self) do
    length(self.waitfor) < div(length(self.acceptors) + 1, 2)
  end

  defp commander_finished(self) do
    self |> Monitor.notify(:COMMANDER_FINISHED)
    Process.exit(self(), :normal)
  end
end
