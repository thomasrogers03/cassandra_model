module CassandraModel
  module SingleTokenBatch

    def keyspace
      nil
    end

    def partition_key
      @statements.first.partition_key
    end

  end
end
