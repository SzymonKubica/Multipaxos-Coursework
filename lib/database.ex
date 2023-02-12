
# distributed algorithms, n.dulay 31 jan 2023
# coursework, paxos made moderately complex

defmodule Database do

# ________________________________________________________ Setters
def seqnum(self, v)     do Map.put(self, :seqnum, v) end
def balance(self, k, v) do Map.put(self, :balances, Map.put(self.balances, k, v)) end

def start config do
  self = %{ config: config,  balances: Map.new,  seqnum: 0 }
  self |> next()
end # start

defp next(self) do
  Debug.letter(self.config, "D")
  receive do
  { :EXECUTE, transaction } ->
    { :MOVE, amount, account1, account2 } = transaction
    self = self |> balance(account1, Map.get(self.balances, account1, 0) + amount )
    self = self |> balance(account2, Map.get(self.balances, account2, 0) - amount )
    self = self |> seqnum(self.seqnum + 1)
    send self.config.monitor, { :DB_MOVE, self.config.node_num, self.seqnum, transaction }
    self |> next()

  unexpected ->
    Helper.node_halt "Database: unexpected message #{inspect unexpected}"
  end # receive
end # next

end # Database
