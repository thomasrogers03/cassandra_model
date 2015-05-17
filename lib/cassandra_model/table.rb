module CassandraModel
  class Table
    extend CassandraModel::Connection

    attr_reader :name

    def initialize(name)
      @name = name.to_s
    end

    def connection
      self.class.connection
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
      @table ||= keyspace.table(name)
    end

    def keyspace
      @keyspace ||= begin
        connection
        cluster.keyspace(config[:keyspace])
      end
    end

    def config
      self.class.config
    end

    def cluster
      self.class.cluster
    end

  end
end