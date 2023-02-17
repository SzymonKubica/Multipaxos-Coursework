# distributed algorithms, n.dulay, 31 jan 2023
# coursework, paxos made moderately complex

defmodule Configuration do
  def node_init do
    # get node arguments and spawn a process to exit node after max_time
    config = %{
      node_suffix: Enum.at(System.argv(), 0),
      timelimit: String.to_integer(Enum.at(System.argv(), 1)),
      debug_level: String.to_integer(Enum.at(System.argv(), 2)),
      n_servers: String.to_integer(Enum.at(System.argv(), 3)),
      n_clients: String.to_integer(Enum.at(System.argv(), 4)),
      param_setup: :"#{Enum.at(System.argv(), 5)}",
      start_function: :"#{Enum.at(System.argv(), 6)}"
    }

    spawn(Helper, :node_exit_after, [config.timelimit])
    config |> Map.merge(Configuration.params(config.param_setup))
  end

  # node_init

  def node_info(config, node_type, node_num \\ "") do
    Map.merge(
      config,
      %{
        node_type: node_type,
        node_num: node_num,
        node_name: "#{node_type}#{node_num}",
        node_location: Helper.node_string(),
        # for ordering output lines
        line_num: 0
      }
    )
  end

  # node_info

  # -----------------------------------------------------------------------------

  def params(:default) do
    %{
      # max requests each client will make
      max_requests: 5,
      # time (ms) to sleep before sending new request
      client_sleep: 2,
      # time (ms) to stop sending further requests
      client_stop: 15_000,
      # :round_robin, :quorum or :broadcast
      send_policy: :round_robin,
      # number of active bank accounts (init balance=0)
      n_accounts: 100,
      # max amount moved between accounts
      max_amount: 1_000,
      # print summary every print_after msecs (monitor)
      print_after: 1_000,
      # multi-paxos window size
      window_size: 10,
      # server_num => crash_after_time(ms)
      crash_servers: %{},
      # verbose_logging: [:replica, :leader, :commander, :acceptor, :scout]
      verbose_logging: [:replica, :leader, :commander]
      # verbose_logging: []
    }

    # redact: performance/liveness/distribution parameters
  end

  # params :default

  # -----------------------------------------------------------------------------

  # crash 2 servers
  def params(:crash2) do
    Map.merge(
      params(:default),
      %{
        # %{ server_num => crash_after_time, ...}
        crash_servers: %{
          3 => 1_500,
          5 => 2_500
        }
      }
    )
  end

  # params :crashes

  def params(:tenk) do
    Map.merge(
      params(:default),
      %{
        # redact definitions
      }
    )
  end

  # params :tenk

  # redact params functions...
end

# Configuration ----------------------------------------------------------------
