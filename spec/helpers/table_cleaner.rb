module CassandraModel
  class Table
    def self.reset!
      # base class
      @table_name = nil
      @save_query = nil
      @delete_qeury = nil
      @partition_key = nil
      @clustering_columns = nil
      @columns = nil

      Connection.reset!
    end

    def partition_key=(columns)
      @partition_key = columns
    end

    def clustering_columns=(columns)
      @clustering_columns = columns
    end

    def columns=(columns)
      @columns = columns
    end
  end
end