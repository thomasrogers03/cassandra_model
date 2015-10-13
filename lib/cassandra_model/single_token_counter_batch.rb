module CassandraModel
  class SingleTokenCounterBatch < Cassandra::Statements::Batch::Counter
    include SingleTokenBatch
  end
end
