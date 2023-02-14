defmodule BallotNumber do
  defstruct value: 0, leader_index: 0, is_bottom: false

  def compare(
        %BallotNumber{value: v1, leader_index: l1, is_bottom: bot1},
        %BallotNumber{value: v2, leader_index: l2, is_bottom: bot2}
      ) do
    # Handle bottom cases first.
    cond do
      bot1 and bot2 -> :eq
      bot1 -> :lt
      bot2 -> :gt
    end

    # If neither ballot number is the bottom, we first compare their values.
    cond do
      v1 < v2 -> :lt
      v1 > v2 -> :gt
    end

    # If the values are equal we compare the leader indices
    cond do
      l1 < l2 -> :lt
      l1 > l2 -> :gt
    end

    :eq
  end

  def bottom() do
    %BallotNumber{is_bottom: true}
  end
end
