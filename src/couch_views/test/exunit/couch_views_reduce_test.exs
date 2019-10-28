defmodule CouchViewsReduceTest do
  use Couch.Test.ExUnit.Case

  alias Couch.Test.Utils

  alias Couch.Test.Setup

  alias Couch.Test.Setup.Step

  setup_all do
    test_ctx = :test_util.start_couch([:fabric, :couch_js, :couch_views, :couch_jobs])

    on_exit(fn ->
      :test_util.stop_couch(test_ctx)
    end)
  end

  setup do
    db_name = Utils.random_name("db")

    admin_ctx =
      {:user_ctx,
       Utils.erlang_record(:user_ctx, "couch/include/couch_db.hrl", roles: ["_admin"])}

    {:ok, db} = :fabric2_db.create(db_name, [admin_ctx])

    docs = create_docs()
    ddoc = create_ddoc()

    {ok, _} = :fabric2_db.update_docs(db, [ddoc | docs])

    on_exit(fn ->
      :fabric2_db.delete(db_name, [admin_ctx])
    end)

    %{
      :db_name => db_name,
      :db => db,
      :ddoc => ddoc
    }
  end

  test "group=true count reduce", context do
    args = %{
      :reduce => true,
      :group => true
      #            :limit => 9
    }

    {:ok, res} = run_query(context, args, "baz")
    IO.inspect(res, label: "OUT")

    assert res == [
             {:row, [key: 1, value: 2]},
             {:row, [key: 2, value: 2]},
             {:row, [key: 3, value: 2]},
             {:row, [key: [1, 1], value: 1]},
             {:row, [key: [1, 1, 5], value: 1]},
             {:row, [key: [1, 2, 6], value: 1]},
             {:row, [key: [2, 1], value: 1]},
             {:row, [key: [2, 3, 6], value: 1]},
             {:row, [key: [3, 1], value: 1]},
             {:row, [key: [3, 1, 5], value: 1]},
             {:row, [key: [3, 4, 5], value: 1]}
           ]
  end

  #  test "group=1 count reduce", context do
  #    args = %{
  #      :reduce => true,
  #      :group_level => 1
  #      #          :limit => 6
  #    }
  #
  #    {:ok, res} = run_query(context, args, "baz")
  #    IO.inspect(res, label: "OUT")
  #
  #    assert res == [
  #             {:row, [key: 1, value: 2]},
  #             {:row, [key: 2, value: 2]},
  #             {:row, [key: 3, value: 2]},
  #             {:row, [key: [1], value: 2]},
  #             {:row, [key: [2], value: 2]},
  #             {:row, [key: [3], value: 2]}
  #           ]
  #  end
  #
  #  test "group=2 count reduce", context do
  #    args = %{
  #      :reduce => true,
  #      :group_level => 2,
  #      :limit => 9
  #    }
  #
  #    {:ok, res} = run_query(context, args, "baz")
  #    IO.inspect(res, label: "OUT")
  #
  #    assert res == [
  #             {:row, [key: 1, value: 2]},
  #             {:row, [key: 2, value: 2]},
  #             {:row, [key: 3, value: 2]},
  #             {:row, [key: [1, 1], value: 2]},
  #             {:row, [key: [1, 2], value: 1]},
  #             {:row, [key: [2, 1], value: 1]},
  #             {:row, [key: [2, 3], value: 1]},
  #             {:row, [key: [3, 1], value: 2]},
  #             {:row, [key: [3, 4], value: 1]}
  #           ]
  #  end
  #
  #  test "group=2 count reduce with limit = 3", context do
  #    args = %{
  #      :reduce => true,
  #      :group_level => 2,
  #      :limit => 4
  #    }
  #
  #    {:ok, res} = run_query(context, args, "baz")
  #    IO.inspect(res, label: "OUT")
  #
  #    assert res == [
  #             {:row, [key: 1, value: 2]},
  #             {:row, [key: 2, value: 2]},
  #             {:row, [key: 3, value: 2]},
  #             {:row, [key: [1, 1], value: 1]}
  #           ]
  #  end
  #
  #  # [
  #  #  row: [key: [2019, 1, 2], value: 1],
  #  #  row: [key: [2019, 1, 4], value: 1],
  #  #  row: [key: [2019, 2, 1], value: 1],
  #  #  row: [key: [2019, 2, 3], value: 1]
  #  # ]
  #
  #  test "group=2 count reduce with startkey", context do
  #    args = %{
  #      #          :reduce => true,
  #      #          :group_level => 2,
  #      :start_key => [2019, 1, 4]
  #      #          :limit => 4
  #    }
  #
  #    {:ok, res} = run_query(context, args, "boom")
  #    IO.inspect(res, label: "OUT")
  #
  #    assert res == [
  #             {:row, [key: [2019, 1], value: 1]},
  #             {:row, [key: [2019, 2], value: 2]}
  #           ]
  #  end

  #  test "group_level=0 _sum reduce", context do
  #    args = %{
  #      :reduce => true,
  #      :group_level => 0
  #      #            :limit => 9
  #    }
  #
  #    {:ok, res} = run_query(context, args, "max")
  #    IO.inspect(res, label: "OUT")
  #
  #    assert res == [
  #             {:row, [key: :null, value: 3]}
  #           ]
  #  end

  defp run_query(context, args, view) do
    db = context[:db]
    ddoc = context[:ddoc]

    :couch_views.query(db, ddoc, view, &__MODULE__.default_cb/2, [], args)
  end

  def default_cb(:complete, acc) do
    {:ok, Enum.reverse(acc)}
  end

  def default_cb({:final, info}, []) do
    {:ok, [info]}
  end

  def default_cb({:final, _}, acc) do
    {:ok, acc}
  end

  def default_cb({:meta, _}, acc) do
    {:ok, acc}
  end

  def default_cb(:ok, :ddoc_updated) do
    {:ok, :ddoc_updated}
  end

  def default_cb(row, acc) do
    {:ok, [row | acc]}
  end

  defp create_docs() do
    for i <- 1..1 do
      group =
        if rem(i, 3) == 0 do
          "first"
        else
          "second"
        end

      :couch_doc.from_json_obj(
        {[
           {"_id", "doc-id-#{i}"},
           {"value", i},
           {"some", "field"},
           {"group", group}
         ]}
      )
    end
  end

  defp create_ddoc() do
    :couch_doc.from_json_obj({[
       {"_id", "_design/bar"},
       {"views",
        {[
           {"baz",
            {[
               {"map",
                """
                function(doc) {
                  emit(doc.value, doc.value);
                  emit(doc.value, doc.value);
                  emit([doc.value, 1], doc.value);
                  emit([doc.value, doc.value + 1, doc.group.length], doc.value);

                  if (doc.value === 3) {
                    emit([1, 1, 5], 1);
                    emit([doc.value, 1, 5], 1);
                  }
                 }
                """},
               {"reduce", "_count"}
             ]}}
           #             {"boom",
           #              {[
           #                 {"map",
           #                  """
           #                  function(doc) {
           #                      var month = 1;
           #                      if (doc.value % 2) {
           #                          month = 2;
           #                      }
           #                      emit([2019, month, doc.value], doc.value);
           #                  }
           #                  """},
           #                 {"reduce", "_count"}
           #               ]}},
           #             {"max",
           #              {[
           #                 {"map",
           #                  """
           #                  function(doc) {
           #                      //emit(doc.value, doc.value);
           #                      //emit([doc.value, 1], doc.value);
           #                      //emit([doc.value, doc.value + 1, doc.group.length], doc.value);
           #                        emit(1, 1);
           #                        emit(2, 2);
           #                        emit(3, 3);
           #                        emit(4, 4);
           #
           #                       emit([2019, 2, 2], 1);
           #                       emit([2019, 3, 3], 2);
           #                       emit([2019, 3, 3], 3);
           #                       emit([2019, 4, 3], 4);
           #                       emit([2019, 5, 3], 6);
           #                      if (doc.value === 3) {
           #                       //emit([doc.value, 1, 5], 1);
           #                      }
           #                  }
           #                  """},
           #                 {"reduce", "_stats"}
           #               ]}}
         ]}}
     ]})
  end
end
