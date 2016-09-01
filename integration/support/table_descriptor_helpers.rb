module CassandraModel
  class TableDescriptor < CassandraModel::Record
    self.connection_name = :single
  end
end
