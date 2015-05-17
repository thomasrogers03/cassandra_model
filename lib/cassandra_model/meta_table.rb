module CassandraModel
  class MetaTable < Table
    def initialize(table_definition)
      @table_definition = table_definition
    end

    def name
      @table_definition.name.to_s
    end

    private

    def table
      @table ||= begin
        descriptor = TableDescriptor.create(@table_definition)
        connection.execute(@table_definition.to_cql) if descriptor.valid
        keyspace.table(name)
      end
    end
  end
end