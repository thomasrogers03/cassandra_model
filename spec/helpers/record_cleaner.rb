module CassandraModel
  class Record
    def self.reset!
      # base class
      @table = nil
      @save_query = nil
      @delete_qeury = nil

      # meta columns
      @deferred_column_writers = nil
      @async_deferred_column_readers = nil
      @async_deferred_column_writers = nil

      # composite columns
      @composite_columns = nil
      @composite_pk_map = nil
      @composite_ck_map = nil
      @composite_defaults = nil

      Connection.reset!
    end

    def self.partition_key=(columns)
      table.partition_key = columns
    end

    def self.clustering_columns=(columns)
      table.clustering_columns = columns
    end

    def self.columns=(columns)
      table.columns = columns
    end
  end
end