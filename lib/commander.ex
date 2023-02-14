# Szymon Kubica (sk4520) 12 Feb 2023
defmodule Commander do
  def start(l, acceptors, replicas, pvalue) do
    waitfor = acceptors

    self = %{
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
        if b == ballot_num do
          self = self |> remove_acceptor_from_waitfor(a)

          if majority_responded?(self) do
            for replica <- self.replicas do
              send(replica, {:DECISION, s, c})
            end
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

  def remove_acceptor_from_waitfor(self, a) do
    %{self | waitfor: List.delete(self.waitfor, a)}
  end
end
