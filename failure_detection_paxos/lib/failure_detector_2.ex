defmodule FailureDetector2 do
  # ____________________________________________________________________ Setters

  defp increase_timeout(self) do
    base_value = max(self.timeout, self.config.initial_leader_timeout)

    %{
      self
      | timeout:
          min(
            self.config.max_leader_timeout,
            round(base_value * self.config.leader_timeout_increase_factor)
          )
    }
  end

  defp decrease_timeout(self) do
    new_timeout =
      max(
        self.config.min_leader_timeout,
        self.timeout - self.config.leader_timeout_decrease_const
      )

    %{
      self
      | timeout: new_timeout
    }
  end

  defp reset_timeout(self) do
    %{self | timeout: self.config.min_leader_timeout}
  end

  # ____________________________________________________________________________

  def start(config, l) do
    self = %{
      type: :failure_detector,
      config: config,
      leader: l,
      timeout: config.initial_leader_timeout
    }

    self |> next
  end

  defp next(self) do
    receive do
      {:PING, ballot_num} ->
        self |> ping(ballot_num)
    end
    |> next
  end

  defp ping(self, ballot_num) do
    preempting_leader = ballot_num.leader
    send(preempting_leader, {:RESPONSE_REQUESTED, self()})
    self |> Monitor.notify(:PING_SENT)

    receive do
      {:STILL_ALIVE} ->
        Process.sleep(self.timeout)

        self
        |> Monitor.notify(:PING_RESPONSE_RECEIVED)
        |> ping(ballot_num)

      {:PREEMPTED_BY, b} ->
        Process.sleep(self.timeout)

        self
        |> Monitor.notify(:PING_RESPONSE_RECEIVED)
        |> ping(b)
    after
      self.timeout ->
        send(self.leader, {:PREEMPT, ballot_num})

        self
        |> Monitor.notify(:FAILURE_DETECTOR_FINISHED)
        |> decrease_timeout
    end
  end
end
