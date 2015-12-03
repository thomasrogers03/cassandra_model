module CassandraModel
  module TableDebug
    Debug = Struct.new(
        :name,
        :table,
        :connection_name,
        :connection,
        :partition_key,
        :clustering_columns,
        :primary_key,
        :columns,
        :allows_truncation?,
    )

    def debug
      Debug.new(
          name,
          table,
          @connection_name,
          connection,
          partition_key,
          clustering_columns,
          primary_key,
          columns,
          !!@allow_truncation,
      )
    end
  end
end
