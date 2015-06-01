module CassandraModel
  class TableRedux
    extend Forwardable

    def_delegator :@connection, :connection
    attr_reader :name

    def initialize(connection_name, table_name)
      @name = table_name
      @connection = ConnectionCache[connection_name]
    end
  end
end