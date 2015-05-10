module CassandraModel
  class Record
    def self.reset!
      @table_name = nil
      @save_query = nil
      @delete_qeury = nil
      @partition_key = nil
      @clustering_columns = nil
      @columns = nil

      @deferred_column_writers = nil
      @async_deferred_column_readers = nil
      @async_deferred_column_writers = nil

      Connection.reset!
    end

    def self.columns=(columns)
      @columns = columns
    end
  end
end