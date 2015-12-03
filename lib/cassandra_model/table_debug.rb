module CassandraModel
  module TableDebug
    Debug = Struct.new(
        :name,
        :table,
        :rotating_tables,
        :first_table,
        :connection_name,
        :connection,
        :partition_key,
        :clustering_columns,
        :primary_key,
        :columns,
        :allows_truncation?,
        :rotating_schedule,
    )

    def debug
      first_table = (@tables.first if @tables)
      Debug.new(
          name,
          table,
          @tables,
          first_table,
          @connection_name,
          connection,
          partition_key,
          clustering_columns,
          primary_key,
          columns,
          !!@allow_truncation,
          @schedule,
      )
    end
  end
end
