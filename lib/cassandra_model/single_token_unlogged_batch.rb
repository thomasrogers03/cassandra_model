module CassandraModel
  class SingleTokenUnloggedBatch < Cassandra::Statements::Batch::Unlogged

    def keyspace
      nil
    end

    def partition_key
      @statements.first.partition_key
    end

  end
end
