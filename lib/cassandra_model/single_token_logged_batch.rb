module CassandraModel
  class SingleTokenLoggedBatch < Cassandra::Statements::Batch::Logged
    include SingleTokenBatch
  end
end
