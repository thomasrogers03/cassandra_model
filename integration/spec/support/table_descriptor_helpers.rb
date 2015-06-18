module CassandraModel
  class TableDescriptor < CassandraModel::Record
    self.connection_name = :single

    #noinspection RubyClassMethodNamingConvention
    def self.create_descriptor_table_if_not_exists
      create_descriptor_table unless table.connection.keyspace.table(table_name)
    end
  end
end