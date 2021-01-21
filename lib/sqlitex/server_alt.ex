defmodule Sqlitex.ServerAlt do
  use GenServer

  alias Sqlitex.Config
  alias Sqlitex.Query
  alias Sqlitex.Statement

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

  # -- Client API -------------------------------------------------------------

  def exec(server, sql, opts \\ []) when is_binary(sql) do
    GenServer.call(server, {:exec, sql}, Config.call_timeout(opts))
  end

  def query(server, query, opts \\ [])

  def query(server, sql, opts) when is_binary(sql) do
    GenServer.call(server, {:query, sql, opts}, Config.call_timeout(opts))
  end

  def query(server, stmt_key, opts) when is_atom(stmt_key) do
    GenServer.call(server, {:query_stored, stmt_key, opts}, Config.call_timeout(opts))
  end

  def query_rows(server, query, opts \\ [])

  def query_rows(server, sql, opts) when is_binary(sql) do
    GenServer.call(server, {:query_rows, sql, opts}, Config.call_timeout(opts))
  end

  def query_rows(server, stmt_key, opts) when is_atom(stmt_key) do
    GenServer.call(server, {:query_rows_stored, stmt_key, opts}, Config.call_timeout(opts))
  end

  def add_statement(server, key, sql, opts \\ []) when is_atom(key) and is_binary(sql) do
    GenServer.call(server, {:add_statement, key, sql, opts}, Config.call_timeout(opts))
  end

  # -- Server implementation --------------------------------------------------

  def init({db_path, config}) do
    case Sqlitex.open(db_path, config) do
      {:ok, db} -> init_state(db, config)
      {:error, reason} -> {:stop, reason}
    end
  end

  defp init_state(db, config) do
    {:ok, %{db: db, config: config, stmap: %{}}}
  end

  def handle_call({:exec, sql}, _from, %{db: db, config: config} = state) do
    result = Sqlitex.exec(db, sql, config)
    {:reply, result, state}
  end

  def handle_call({:query, sql, opts}, _from, %{db: db, config: config} = state) do
    result = Query.query(db, sql, Keyword.merge(config, opts))
    {:reply, result, state}
  end

  def handle_call({:query_rows, sql, opts}, _from, %{db: db, config: config} = state) do
    result = Query.query_rows(db, sql, Keyword.merge(config, opts))
    {:reply, result, state}
  end

  def handle_call({:query_stored, key, opts}, _from, %{stmap: stmap} = state)
      when is_map_key(stmap, key) do
    %{stmap: stmap, config: config} = state

    opts = Keyword.merge(config, opts)

    reply =
      with {:ok, stmt} <- Statement.bind_values(stmap[key], Keyword.get(opts, :bind, []), opts),
           {:ok, res} <- Statement.fetch_all(stmt, opts),
           do: {:ok, res}

    # statements are mutable values, so we do not have to store the bound
    # statement in the state.
    {:reply, reply, state}
  end

  def handle_call({:query_stored, key, _opts}, _from, state) do
    {:reply, {:error, {:unknown_query, key}}, state}
  end

  def handle_call({:query_rows_stored, key, opts}, _from, %{stmap: stmap} = state)
      when is_map_key(stmap, key) do
    %{stmap: stmap, config: config} = state

    opts = Keyword.merge(config, opts)

    reply =
      with {:ok, stmt} <- Statement.bind_values(stmap[key], Keyword.get(opts, :bind, []), opts),
           {:ok, rows} <- Statement.fetch_all(stmt, Keyword.put(opts, :into, :raw_list)),
           do: {:ok, %{rows: rows, columns: stmt.column_names, types: stmt.column_types}}

    {:reply, reply, state}
  end

  def handle_call({:query_rows_stored, key, _opts}, _from, state) do
    {:reply, {:error, {:unknown_query, key}}, state}
  end

  def handle_call({:add_statement, key, _, _}, _from, %{stmap: stmap} = state)
      when is_map_key(stmap, key) do
    {:reply, {:error, {:key_exists, key}}, state}
  end

  def handle_call({:add_statement, key, sql, opts}, _from, %{db: db, config: config} = state) do
    case Statement.prepare(db, sql, Keyword.merge(config, opts)) do
      {:ok, stmt} -> {:reply, :ok, put_in(state.stmap[key], stmt)}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  # -- Server side code -------------------------------------------------------
end
