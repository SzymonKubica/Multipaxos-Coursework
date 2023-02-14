defmodule Pvalue do
  defstruct ballot_number: BallotNumber.bottom(), slot_number: 0, command: nil

  def compare(
        %Pvalue{ballot_number: b1, slot_number: _s1, command: _c1},
        %Pvalue{ballot_number: b2, slot_number: _s2, command: _c2}
      ) do
    cond do
      b1 < b2 -> :lt
      b1 > b2 -> :gt
      b1 == b2 -> :eq
    end
  end
end
