module CassandraModel
  module Connection
    def self.reset!
      @@config = nil
      @@connection = nil
      @@cluster = nil
      @@statement_cache = {}
      @@keyspace = nil
    end
  end

  class ConnectionCache
    def self.reset!
      @@cache.clear
    end
  end
end