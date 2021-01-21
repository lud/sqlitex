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
end
