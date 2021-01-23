defmodule Sqlitex.ServerAltTest do
  use ExUnit.Case
  alias Sqlitex.ServerAlt, as: Server

  test "base query functionality" do
    {:ok, server} = Server.start_link(:memory)
    assert :ok === Server.exec(server, "create table foo(id integer)")
    assert :ok === Server.exec(server, "insert into foo (id) values (42)")
    assert {:ok, [[{:id, 42}]]} === Server.query(server, "select * from foo")
    assert :ok === Server.exec(server, "insert into foo (id) values (99), (1)")

    assert {:ok, [%{id: 1}, %{id: 42}, %{id: 99}]} ===
             Server.query(server, "select * from foo order by id", into: %{})

    assert {:ok, [%{id: 1}, %{id: 42}]} ===
             Server.query(server, "select * from foo where id < ?1order by id",
               bind: [50],
               into: %{}
             )

    assert {:error, {:sqlite_error, 'no such table: unexisting'}} =
             Server.query(server, "select * from unexisting")
  end

  test "base query_rows functionality" do
    {:ok, server} = Server.start_link(:memory)
    assert :ok === Server.exec(server, "create table foo(id integer)")
    assert :ok === Server.exec(server, "insert into foo (id) values (42)")

    assert {:ok, %{columns: [:id], rows: [[42]], types: [:integer]}} ===
             Server.query_rows(server, "select id from foo")

    assert :ok === Server.exec(server, "insert into foo (id) values (99), (1)")

    assert {:ok, %{columns: [:id], rows: [[1], [42], [99]], types: [:integer]}} ===
             Server.query_rows(server, "select * from foo order by id", into: %{})

    assert {:ok, %{columns: [:id], rows: [[1], [42]], types: [:integer]}} ===
             Server.query_rows(server, "select * from foo where id < ?1order by id",
               bind: [50],
               into: %{}
             )

    assert {:error, {:sqlite_error, 'no such table: unexisting'}} =
             Server.query(server, "select * from unexisting")
  end

  test "server managed statements" do
    {:ok, server} = Server.start_link(:memory)
    assert :ok === Server.exec(server, "create table foo(id integer)")
    assert :ok === Server.add_statement(server, :add_foo, "insert into foo (id) values (?1)")
    assert :ok === Server.add_statement(server, :get_all_foos, "select * from foo")
    assert :ok === Server.add_statement(server, :get_foo, "select * from foo where id = ?1")

    assert {:error, {:key_exists, :add_foo}} ===
             Server.add_statement(server, :add_foo, "insert into foo (id) values (?1)")

    assert {:ok, []} === Server.query(server, "select * from foo")
    assert {:ok, []} === Server.query(server, :add_foo, bind: [1])
    assert {:ok, []} === Server.query(server, :add_foo, bind: [2])

    assert {:ok, [[{:id, 1}], [{:id, 2}]]} = Server.query(server, :get_all_foos)

    assert {:ok, %{columns: [:id], rows: [[2]], types: [:integer]}} ===
             Server.query_rows(server, :get_foo, bind: [2])
  end

  test "transaction with commit" do
    {:ok, server} = Server.start_link(:memory)
    assert :ok === Server.exec(server, "create table foo(id integer)")
    assert :ok === Server.add_statement(server, :ids_sorted, "select id from foo order by id")

    all_ids = fn handle ->
      {:ok, %{rows: rows}} = Server.query_rows(handle, :ids_sorted)
      :lists.flatten(rows)
    end

    assert [] === all_ids.(server)

    return_val =
      Server.transaction(server, fn trans ->
        Server.exec!(trans, "insert into foo (id) values (42)")
        # changes are visible inside the transaction
        assert [42] === all_ids.(trans)

        "return-val"
      end)

    assert return_val === "return-val"

    # changes are visible after the transaction
    assert [42] === all_ids.(server)
  end

  test "transaction with rollback" do
    {:ok, server} = Server.start_link(:memory)
    assert :ok === Server.exec(server, "create table foo(id integer)")
    assert :ok === Server.add_statement(server, :ids_sorted, "select id from foo order by id")

    all_ids = fn handle ->
      {:ok, %{rows: rows}} = Server.query_rows(handle, :ids_sorted)
      :lists.flatten(rows)
    end

    assert [] === all_ids.(server)

    return_val =
      try do
        Server.transaction(server, fn trans ->
          Server.exec!(trans, "insert into foo (id) values (42)")
          # changes are visible inside the transaction, before rollback
          assert [42] === all_ids.(trans)

          throw(:fail)
        end)
      catch
        :throw, :fail -> :failed
      end

    assert return_val === :failed

    # changes were rolled back
    assert [] === all_ids.(server)
  end

  test "calling the server in a transaction is forbidden" do
    {:ok, server} = Server.start_link(:memory)
    assert :ok === Server.exec(server, "create table foo(id integer)")

    Server.transaction(server, fn trans ->
      # calling the server inside the transaction
      assert {:error, :bad_context} = Server.exec(server, "select * from foo")
      # nested transaction on the server
      assert {:error, :bad_context} = Server.transaction(server, "select * from foo")
      # nested transaction on the transaction
      try do
        Server.transaction(trans, "select * from foo")
        flunk("nested transaction was accepted")
      rescue
        e in ArgumentError -> assert true
      end
    end)

    # After the transaction, the process flag is cleaned up
    assert {:ok, _} = Server.query(server, "select * from foo")
  end
end
