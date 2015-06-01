module CassandraModel
  class TableRedux
    extend Forwardable

    def_delegator :@connection, :connection
    attr_reader :name

    def initialize(connection_name, table_name)
      @name = table_name
      @connection = ConnectionCache[connection_name]
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