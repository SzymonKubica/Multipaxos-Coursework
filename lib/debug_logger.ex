defmodule DebugLogger do
  def log(config, type, id_line, message) do
    if config.debug_level > 1 and Enum.member?(config.verbose_logging, type) do
      IO.puts(id_line <> "\n--> " <> message <> "\n")
    end
  end
end
