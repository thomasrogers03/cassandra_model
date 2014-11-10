class Record
  DEFAULT_CONFIGURATION = {
      'host' => 'localhost',
      'keyspace' => 'default_keyspace',
      'port' => '9042'
  }

  class << self
    def table_name=(value)
      @table_name = value
    end

    def table_name
      @table_name ||= self.to_s.underscore.pluralize
    end

    def config=(value)
      @@config = DEFAULT_CONFIGURATION.merge(value)
    end

    def config
      @@config ||= DEFAULT_CONFIGURATION
    end

    def connection
      @@connection ||= Cassandra.cluster(hosts: config['host'], connect_timeout: 120)
    end
  end
end