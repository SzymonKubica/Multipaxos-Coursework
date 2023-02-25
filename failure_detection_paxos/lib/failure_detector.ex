defmodule FailureDetector do
  def start(config, l, timeout) do
    self = %{
      type: :scout,
      config: config,
      leader: l,
      timeout: timeout
    }

    self |> next
  end

  defp next(self) do
  end
end
