defmodule Mssqlex.StoredProcedureTest do
  use ExUnit.Case, async: true

  alias Mssqlex.Result

  setup_all do
    {:ok, pid} = Mssqlex.start_link([])
    Mssqlex.query!(pid, "DROP DATABASE IF EXISTS sp_test;", [])
    {:ok, _, _} = Mssqlex.query(pid, "CREATE DATABASE sp_test;", [])

    {:ok, _, %Result{}} = Mssqlex.query(pid,
      "CREATE TABLE sp_test.dbo.simple_select (name nvarchar(50));", [])
    {:ok, _, %Result{num_rows: 2}} = Mssqlex.query(pid,
      ["INSERT INTO sp_test.dbo.simple_select (name) VALUES ('Steven'), ('Bob');"], [])

    {:ok, [pid: pid]}
  end

  test "return a resultset from a select", %{pid: pid} do
    Mssqlex.query(pid, "USE sp_test;", [])

    {:ok, _, %Result{}} = Mssqlex.query(pid, """
      CREATE PROCEDURE dbo.select_all AS 
      SELECT  * 
      FROM    sp_test.dbo.simple_select
      ORDER BY name DESC
    """, [])

    assert {:ok, _, %Result{columns: ["name"], num_rows: 2, rows: [["Steven"], ["Bob"]]}}
      = Mssqlex.query(pid, "EXEC select_all;", [])
  end

  test "return multiple resultsets from a select", %{pid: pid} do
    Mssqlex.query(pid, "USE sp_test;", [])

    {:ok, _, %Result{}} = Mssqlex.query(pid, """
      CREATE PROCEDURE dbo.select_three AS 
      SELECT * FROM sp_test.dbo.simple_select WHERE name = 'Steven'
      SELECT * FROM sp_test.dbo.simple_select WHERE name = 'Bob'
      SELECT * FROM sp_test.dbo.simple_select ORDER BY name ASC
    """, [])

    assert {:ok, %Mssqlex.Query{columns: nil, name: "", statement: "EXEC select_three;"},
                [%Mssqlex.Result{columns: ["name"], num_rows: 1, rows: [["Steven"]]},
                 %Mssqlex.Result{columns: ["name"], num_rows: 1, rows: [["Bob"]]},
                 %Mssqlex.Result{columns: ["name"], num_rows: 2, rows: [["Bob"], ["Steven"]]}
                ]}
      == Mssqlex.query(pid, "EXEC select_three;", [], parameterized: false)
  end

  test "handle input/output parameters", %{pid: pid} do
    Mssqlex.query(pid, "USE sp_test;", [])

    {:ok, _, %Result{}} = Mssqlex.query(pid, """
      CREATE PROCEDURE dbo.input_ouput_params 
        @p_in NUMERIC(10, 0),
        @p_out NUMERIC(10, 0) OUTPUT,
        @p_in_out VARCHAR(50) OUTPUT
      AS 
        SET @p_out = @p_in * 2
        SET @p_in_out = UPPER(@p_in_out)
    """, [])
    assert {:ok, _, %Result{columns: [], num_rows: 1, rows: [[6, "TEST"]]}}
      = Mssqlex.query(pid, "EXEC input_ouput_params ?, ?, ?", [in: 3, out: 0, inout: "test"])
  end

  test "return nothing", %{pid: pid} do
    Mssqlex.query(pid, "USE sp_test;", [])

    {:ok, _, %Result{}} = Mssqlex.query(pid, """
      CREATE PROCEDURE dbo.return_nothing 
      AS 
        return
    """, [])
    assert {:ok, _, %Result{columns: nil, num_rows: :undefined, rows: nil}}
      = Mssqlex.query(pid, "EXEC return_nothing", [])
  end

end
