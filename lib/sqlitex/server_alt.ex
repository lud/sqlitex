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

  @trans_flag {__MODULE__, :in_transaction}

  @opaque local :: %Transaction{} | %State{}
  @type server :: GenServer.server()
  @type context :: local() | server()
  @type sql :: binary()
  @type statement_name :: atom()
  @type query :: sql() | statement_name()
  @type bindings :: list()
  @type row_type :: :raw_list | Collectable.t()
  @type transaction_fun :: (local -> any)

  @type command ::
          {:exec, query()}
          | {:query, query(), bindings}
          | {:query_rows, query(), bindings}
          | {:add_statement, statement_name(), sql()}
          | :add_meta
          | {:fetch_all, row_type()}
          | {:transaction, transaction_fun()}
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

  @spec exec(context, sql, options) :: :ok | {:error, term}
  def exec(context, sql, opts \\ []) do
    call(context, {:exec, sql}, opts)
  end

  @spec exec!(context, sql, options) :: :ok
  def exec!(context, sql, opts \\ []) do
    raise_error(exec(context, sql, opts))
  end

  def query(context, query, opts \\ []) when is_binary(query) when is_atom(query) do
    call(context, {:query, query, get_bindings(opts)}, opts)
  end

  def query!(context, query, opts \\ []) when is_binary(query) when is_atom(query) do
    raise_error(query(context, query, opts))
  end

  def query_rows(context, query, opts \\ []) when is_binary(query) when is_atom(query) do
    call(context, {:query_rows, query, get_bindings(opts)}, opts)
  end

  def query_rows!(context, query, opts) when is_binary(query) when is_atom(query) do
    raise_error(query_rows!(context, query, opts))
  end

  defp get_bindings(opts) do
    Keyword.get(opts, :bind, [])
  end

  def add_statement(server, key, sql, opts \\ []) when is_atom(key) and is_binary(sql) do
    protected_gen_call(server, {:add_statement, key, sql, opts}, Config.call_timeout(opts))
  end

  @spec with_transaction(server, transaction_fun(), options()) :: any
  def with_transaction(server, fun, opts \\ []) do
    transaction(server, fun, opts)
  end

  @spec transaction(server, transaction_fun(), options()) :: any
  def transaction(server, fun, opts \\ [])

  def transaction(%Transaction{}, _, _) do
    raise ArgumentError, message: "nested transactions are not supported"
  end

  def transaction(server, fun, opts) do
    protected_gen_call(server, {:transaction, fun, opts}, Config.call_timeout(opts))
    |> case do
      {:ok, value} -> value
      {:error, _} = err -> err
      {:rescued, error, trace} -> reraise(error, trace)
      {:throw, t} -> throw(t)
      {:exit, reason} -> exit(reason)
    end
  end

  defp protected_gen_call(server, request, timeout) do
    case Process.get(@trans_flag) do
      nil -> GenServer.call(server, request, timeout)
      _ -> {:error, :bad_context}
    end
  end

  defp raise_error(:ok) do
    :ok
  end

  defp raise_error({:ok, data}) do
    data
  end

  defp raise_error({:error, {:sqlite_error, charlist}}) do
    raise to_string(charlist)
  end

  defp raise_error({:error, :bad_context}) do
    raise "invalid context when calling #{__MODULE__} functions."
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

  def handle_call({:transaction, fun, opts}, _from, state) do
    %State{db: db, config: config, stmap: stmap} = state
    trans = %Transaction{db: db, config: Keyword.merge(config, opts), stmap: stmap}
    reply = run_transaction(trans, fun, opts)
    {:reply, reply, state}
  end

  ## -- SQLite operations ----------------------------------------------------

  @spec call(context, command, options) :: :ok | {:ok, any} | {:error, any}

  defp call(%_{config: config} = local, command, opts)
       when is_struct(local, Transaction)
       when is_struct(local, State) do
    opts = Keyword.merge(config, opts)
    handle_command(command, opts, local)
  end

  defp call(server, command, opts) do
    protected_gen_call(server, {:call_command, command, opts}, Config.call_timeout(opts))
  end

  @spec handle_command(command, options, local) :: {:ok, any} | {:error, any}
  defp handle_command(command, options, local)

  defp handle_command({:exec, sql}, opts, local) when is_binary(sql) do
    Sqlitex.exec(local.db, sql, opts)
  end

  defp handle_command({:query, query, bindings}, opts, local) do
    with {:ok, stmt} <- get_statement(local, query, opts),
         {:ok, stmt} <- Statement.bind_values(stmt, bindings, opts),
         do: Statement.fetch_all(stmt, opts)
  end

  defp handle_command({:query_rows, query, bindings}, opts, local) do
    with {:ok, stmt} <- get_statement(local, query, opts),
         {:ok, stmt} <- Statement.bind_values(stmt, bindings, opts),
         {:ok, rows} <- Statement.fetch_all(stmt, Keyword.put(opts, :into, :raw_list)),
         do: {:ok, %{rows: rows, columns: stmt.column_names, types: stmt.column_types}}
  end

  defp handle_command(command, _, _) do
    {:error, {:unknown_command, command}}
  end

  @spec get_statement(local, query, list()) :: {:ok, %Statement{}} | {:error, any}
  defp get_statement(local, query, opts)

  defp get_statement(%{stmap: map}, key, _opts) when is_map_key(map, key) do
    Map.fetch(map, key)
  end

  defp get_statement(%{db: db}, sql, opts) when is_binary(sql) do
    Statement.prepare(db, sql, opts)
  end

  defp run_transaction(trans, fun, opts) do
    with :ok <- exec(trans, "begin", opts),
         {:ok, result} <- call_trans_fun(trans, fun),
         :ok <- exec(trans, "commit", opts) do
      {:ok, result}
    else
      err ->
        :ok = exec(trans, "rollback", opts)
        err
    end
  end

  defp call_trans_fun(trans, fun) do
    Process.put(@trans_flag, true)
    {:ok, fun.(trans)}
  rescue
    error -> {:rescued, error, __STACKTRACE__}
  catch
    :throw, t -> {:throw, t}
    :exit, reason -> {:exit, reason}
  after
    Process.delete(@trans_flag)
  end
end
