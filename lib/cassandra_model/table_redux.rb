module CassandraModel
  class TableRedux
    extend Forwardable

    attr_reader :name

    def initialize(connection_name = nil, table_name)
      @name = table_name.to_s
      @connection_name = connection_name
    end

    #noinspection RubyUnusedLocalVariable
    def in_context(time)
      yield self
    end

    def connection
      ConnectionCache[@connection_name]
    end

    def allow_truncation!
      @allow_truncation = true
    end

    def truncate!
      raise "Truncation not enabled for table '#{name}'" unless @allow_truncation
      connection.session.execute("TRUNCATE #{name}")
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

    def primary_key
      partition_key + clustering_columns
    end

    def columns
      @columns ||= table.columns.map { |column| column.name.to_sym }
    end

    private

    def table
      connection.keyspace.table(name)
    end
  end
end
