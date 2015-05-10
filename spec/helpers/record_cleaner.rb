module CassandraModel
  class Record
    def self.reset!
      # base class
      @table_name = nil
      @save_query = nil
      @delete_qeury = nil
      @partition_key = nil
      @clustering_columns = nil
      @columns = nil

      # meta columns
      @deferred_column_writers = nil
      @async_deferred_column_readers = nil
      @async_deferred_column_writers = nil

      # composite columns
      @composite_columns = nil

      Connection.reset!
    end

    def self.partition_key=(columns)
      @partition_key = columns
    end

    def self.clustering_columns=(columns)
      @clustering_columns = columns
    end

    def self.columns=(columns)
      @columns = columns
    end
  end
end