module CassandraModel
  class Record
    def self.reset!
      # base class
      @attributes = nil
      @table = nil
      @save_query = nil
      @delete_qeury = nil
      @columns = nil

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
  end
end