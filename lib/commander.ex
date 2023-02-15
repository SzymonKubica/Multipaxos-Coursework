# Szymon Kubica (sk4520) 12 Feb 2023
defmodule Commander do
  def start(config, l, acceptors, replicas, pvalue) do
    waitfor = acceptors

    self = %{
      config: config,
      leader: l,
      waitfor: waitfor,
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
    {ballot_num, s, c} = self.pvalue

    receive do
      {:p2b, a, b} ->
        self |> log("Received p2b message for ballot #{inspect(b)}")

        if BallotNumber.compare(b, ballot_num) == :eq do
          self = self |> remove_acceptor_from_waitfor(a)

          if majority_responded?(self) do
            self |> log("Received majority of responses for #{inspect(b)}")

            for replica <- self.replicas do
              send(replica, {:DECISION, s, c})
            end

            send(self.config.monitor, {:COMMANDER_FINISHED, self.config.node_num})
          else
            self |> next
          end
        else
          send(self.leader, {:PREEMPTED, b})
          send(self.config.monitor, {:COMMANDER_FINISHED, self.config.node_num})
        end
    end
  end

  def majority_responded?(self) do
    length(self.waitfor) < length(self.acceptors) / 2
  end

  def remove_acceptor_from_waitfor(self, a) do
    %{self | waitfor: List.delete(self.waitfor, a)}
  end

  defp log(self, message) do
    DebugLogger.log(
      self.config,
      :commander,
      "Commander at #{self.config.node_name}",
      message
    )
  end
end
