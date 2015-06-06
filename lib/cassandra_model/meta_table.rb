module CassandraModel
  class MetaTable < TableRedux
    def initialize(connection_name = nil, table_definition)
      @table_definition = table_definition
      @connection = ConnectionCache[connection_name]
    end

    def reset_local_schema!
      raise Cassandra::Errors::ClientError, 'Schema changes are not supported for meta tables'
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

    def keyspace
      connection.keyspace
    end

    def create_table
      descriptor = TableDescriptor.create(@table_definition)
      connection.session.execute(@table_definition.to_cql) if descriptor.valid
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