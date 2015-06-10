module CassandraModel
  class Record
    def self.reset!
      # all static instance variables should be stored within this struct
      @table_data = nil
      @table_config = nil

      Connection.reset!
    end
  end
end