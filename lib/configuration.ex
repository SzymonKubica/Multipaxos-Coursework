# distributed algorithms, n.dulay, 31 jan 2023
# coursework, paxos made moderately complex

defmodule Configuration do

def node_init do
  # get node arguments and spawn a process to exit node after max_time
  config = %{
    node_suffix:    Enum.at(System.argv, 0),
    timelimit:      String.to_integer(Enum.at(System.argv, 1)),
    debug_level:    String.to_integer(Enum.at(System.argv, 2)),
    n_servers:      String.to_integer(Enum.at(System.argv, 3)),
    n_clients:      String.to_integer(Enum.at(System.argv, 4)),
    param_setup:    :'#{Enum.at(System.argv, 5)}',
    start_function: :'#{Enum.at(System.argv, 6)}',
  }

  spawn(Helper, :node_exit_after, [config.timelimit])
  config |> Map.merge(Configuration.params(config.param_setup))
end # node_init

def node_info(config, node_type, node_num \\ "") do
  Map.merge config,
  %{
    node_type:      node_type,
    node_num:       node_num,
    node_name:      "#{node_type}#{node_num}",
    node_location:  Helper.node_string(),
    line_num:       0,  # for ordering output lines
  }
end # node_info

# -----------------------------------------------------------------------------

def params(:default) do
  %{
  max_requests:  500,           # max requests each client will make
  client_sleep:  2,             # time (ms) to sleep before sending new request
  client_stop:   15_000,        # time (ms) to stop sending further requests
  send_policy:	 :round_robin,  # :round_robin, :quorum or :broadcast

  n_accounts:    100,           # number of active bank accounts (init balance=0)
  max_amount:    1_000,         # max amount moved between accounts

  print_after:   1_000,         # print summary every print_after msecs (monitor)

  window_size:   10,            # multi-paxos window size

  crash_servers: %{             # server_num => crash_after_time(ms)
  },}

  # redact: performance/liveness/distribution parameters

end # params :default

# -----------------------------------------------------------------------------

def params(:crash2) do         # crash 2 servers
  Map.merge (params :default),
  %{
  crash_servers: %{            # %{ server_num => crash_after_time, ...}
    3 => 1_500,
    5 => 2_500,
    },
  }
end # params :crashes

def params(:tenk) do
  Map.merge (params :default),
  %{
    # redact definitions
  }
end # params :tenk

# redact params functions...

end # Configuration ----------------------------------------------------------------


