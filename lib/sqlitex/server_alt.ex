defmodule Sqlitex.ServerAlt do
  use GenServer

  alias Sqlitex.Config
  alias Sqlitex.Query
  alias Sqlitex.Statement

  defmodule Transaction do
    defstruct db: nil, stmap: %{}, config: []
  end

  defmodule State do
    defstruct db: nil, stmap: %{}, config: []
  end

  @opaque context :: %Transaction{} | %State{}
  @type server :: GenServer.server() | context()
  @type sql :: binary()
  @type statement_name :: atom()
  @type query :: sql() | statement_name()
  @type bindings :: list()
  @type row_type :: :raw_list | Collectable.t()
  @type command ::
          {:exec, query()}
          | {:query, query(), bindings}
          | {:query_rows, query(), bindings}
          | {:add_statement, statement_name(), sql()}
          | :add_meta
          | {:fetch_all, row_type()}
  @type options :: [option]
  @type option ::
          Query.query_option()
          | {:call_timeout, pos_integer()}

  # -- Client API -------------------------------------------------------------

  def start_link(db_path, opts \\ [])

  def start_link(:memory, opts) do
    start_link(':memory:', opts)
  end

  def start_link(db_path, opts) do
    config = [
      db_timeout: Config.db_timeout(opts),
      db_chunk_size: Config.db_chunk_size(opts)
    ]

    GenServer.start_link(__MODULE__, {db_path, config}, opts)
  end

  def exec(server, sql, opts \\ []) do
    call(server, {:exec, sql}, opts)
  end

  def query(server, query, opts \\ [])

  def query(server, query, opts) when is_binary(query) when is_atom(query) do
    call(server, {:query, query, get_bindings(opts)}, opts)
  end

  def query_rows(server, query, opts \\ [])

  def query_rows(server, query, opts) when is_binary(query) when is_atom(query) do
    call(server, {:query_rows, query, get_bindings(opts)}, opts)
  end

  defp get_bindings(opts) do
    Keyword.get(opts, :bind, [])
  end

  def add_statement(server, key, sql, opts \\ []) when is_atom(key) and is_binary(sql) do
    GenServer.call(server, {:add_statement, key, sql, opts}, Config.call_timeout(opts))
  end

  # -- GenServer implementation -----------------------------------------------

  def init({db_path, config}) do
    case Sqlitex.open(db_path, config) do
      {:ok, db} -> init_state(db, config)
      {:error, reason} -> {:stop, reason}
    end
  end

  defp init_state(db, config) do
    {:ok, %State{db: db, config: config, stmap: %{}}}
  end

  def handle_call({:call_command, command, opts}, _from, state) do
    result = call(state, command, opts)
    {:reply, result, state}
  end

  def handle_call({:add_statement, key, _, _}, _from, %{stmap: stmap} = state)
      when is_map_key(stmap, key) do
    {:reply, {:error, {:key_exists, key}}, state}
  end

  def handle_call({:add_statement, key, sql, opts}, _from, state) do
    case get_statement(state, sql, Keyword.merge(state.config, opts)) do
      {:ok, stmt} -> {:reply, :ok, put_in(state.stmap[key], stmt)}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  ## -- SQLite operations ----------------------------------------------------

  @spec call(server, command, options) :: {:ok, any} | {:error, any}

  defp call(%_{config: config} = context, command, opts)
       when is_struct(context, Transaction)
       when is_struct(context, State) do
    opts = Keyword.merge(config, opts)
    handle_command(command, opts, context)
  end

  defp call(server, command, opts) do
    GenServer.call(server, {:call_command, command, opts}, Config.call_timeout(opts))
  end

  @spec handle_command(command, options, context) :: {:ok, any} | {:error, any}
  defp handle_command(command, options, context)

  defp handle_command({:exec, sql}, opts, context) when is_binary(sql) do
    Sqlitex.exec(context.db, sql, opts)
  end

  defp handle_command({:query, query, bindings}, opts, context) do
    with {:ok, stmt} <- get_statement(context, query, opts),
         {:ok, stmt} <- Statement.bind_values(stmt, bindings, opts),
         do: Statement.fetch_all(stmt, opts)
  end

  defp handle_command({:query_rows, query, bindings}, opts, context) do
    with {:ok, stmt} <- get_statement(context, query, opts),
         {:ok, stmt} <- Statement.bind_values(stmt, bindings, opts),
         {:ok, rows} <- Statement.fetch_all(stmt, Keyword.put(opts, :into, :raw_list)),
         do: {:ok, %{rows: rows, columns: stmt.column_names, types: stmt.column_types}}
  end

  defp handle_command(command, _, _) do
    {:error, {:unknown_command, command}}
  end

  @spec get_statement(context, query, list()) :: {:ok, %Statement{}} | {:error, any}
  defp get_statement(context, query, opts)

  defp get_statement(%{stmap: map}, key, _opts) when is_map_key(map, key) do
    Map.fetch(map, key)
  end

  defp get_statement(%{db: db}, sql, opts) when is_binary(sql) do
    Statement.prepare(db, sql, opts)
  end
end
