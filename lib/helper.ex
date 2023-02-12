
# distributed algorithms, n.dulay, 31 jan 2023
# coursework, paxos made moderately complex
#
# various helper functions

defmodule Helper do

def lookup(name) do
  addresses = :inet_res.lookup(name,:in,:a)
  {a, b, c, d} = hd(addresses) 		# get octets for 1st ipv4 address
  :"#{a}.#{b}.#{c}.#{d}"
end # lookup

def node_ip_addr do
  {:ok, interfaces} = :inet.getif()		# get interfaces
  {address, _gateway, _mask}  = hd(interfaces)	# get data for 1st interface
  {a, b, c, d} = address   			# get octets for address
  "#{a}.#{b}.#{c}.#{d}"
end # node_ip_addr

def node_string() do
  "#{node()} (#{node_ip_addr()})"
end

def node_exit do 	# nicely stop and exit the node
  System.stop(1)	# System.halt(1) for a hard non-tidy node exit
end # node_exit

def node_halt(message) do
  IO.puts "Exiting Node #{node()} - #{message}"
  node_exit()
end # halt

def node_exit_after(duration) do
  Process.sleep(duration)
  IO.puts "  Node #{node()} exiting - maxtime reached"
  node_exit()
end # exit_after

def node_sleep(message) do
  IO.puts "Node #{node()} Going to Sleep - #{message}"
  Process.sleep :infinity
end # node_sleep

# --------------------------------------------------------------------------

def list_to_map(list) do
  for k <- 0..length(list)-1, into: Map.new do {k, Enum.at(list, k)} end
end # list_to_map

def unzip3(triples) do :lists.unzip3(triples) end
def adler32(x)      do :erlang.adler32(x) end

end # Helper
