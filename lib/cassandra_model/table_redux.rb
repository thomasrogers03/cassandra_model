module CassandraModel
  class TableRedux
    extend Forwardable

    def_delegator :@connection, :connection
    attr_reader :name

    def initialize(connection_name = nil, table_name)
      @name = table_name.to_s
      @connection = ConnectionCache[connection_name]
    end

    def reset_local_schema!
      @partition_key = nil
      @clustering_columns = nil
      @columns = nil
    end

    def partition_key
      @partition_key ||= table.send(:partition_key).map { |column| column.name.to_sym }
    end

    def clustering_columns
      @clustering_columns ||= table.send(:clustering_columns).map { |column| column.name.to_sym }
    end

    def columns
      @columns ||= table.columns.map { |column| column.name.to_sym }
    end

    private

    def table
      @connection.keyspace.table(name)
    end
  end
end