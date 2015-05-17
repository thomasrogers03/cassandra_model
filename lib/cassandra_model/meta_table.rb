module CassandraModel
  class MetaTable < Table
    def initialize(table_definition)
      @table_definition = table_definition
    end

    def name
      @name ||= @table_definition.name_in_cassandra
    end

    private

    def table
      @table ||= begin
        descriptor = TableDescriptor.create(@table_definition)
        connection.execute(@table_definition.to_cql) if descriptor.valid
        until keyspace.table(name)
          @keyspace = nil
          sleep 0.100
        end
        keyspace.table(name)
      end
    end
  end
end