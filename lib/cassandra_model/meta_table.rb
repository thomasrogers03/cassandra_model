module CassandraModel
  class MetaTable < Table
    def initialize(table_definition)
      @table_definition = table_definition
    end

    def name
      @name ||= begin
        create_table
        name_in_cassandra
      end
    end

    private

    def table
      @table ||= create_table
    end

    def create_table
      descriptor = TableDescriptor.create(@table_definition)
      connection.execute(@table_definition.to_cql) if descriptor.valid
      100.times do
        sleep 0.100
        break if keyspace.table(name_in_cassandra)
      end
      keyspace.table(name_in_cassandra) or raise "Could not verify the creation of table #{name_in_cassandra}"
    end

    def name_in_cassandra
      @table_definition.name_in_cassandra
    end

  end
end