module CassandraModel
  class SingleTokenUnloggedBatch < Cassandra::Statements::Batch::Unlogged
    include SingleTokenBatch
  end
end
