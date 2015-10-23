module CassandraModel
  class MetaTable < TableRedux
    def initialize(connection_name = nil, table_definition)
      @table_definition = table_definition
      @connection_name = connection_name
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

    def ==(rhs)
      connection == rhs.connection &&
          table_definition == rhs.table_definition
    end

    protected

    attr_reader :table_definition

    private

    def table
      @table ||= create_table
    end

    def keyspace
      connection.keyspace
    end

    def create_table
      descriptor = TableDescriptor.create(@table_definition)
      create_cassandra_table(descriptor) if descriptor.valid
      100.times do
        sleep 0.100
        break if keyspace.table(name_in_cassandra)
      end
      keyspace.table(name_in_cassandra) or raise "Could not verify the creation of table #{name_in_cassandra}"
    end

    def create_cassandra_table(descriptor)
      begin
        connection.session.execute(create_table_cql)
      rescue
        descriptor.delete
        raise
      end
    end

    def create_table_cql
      @table_definition.to_cql(check_exists: true)
    end

    def name_in_cassandra
      @table_definition.name_in_cassandra
    end

  end
end
