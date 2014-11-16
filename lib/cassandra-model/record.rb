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
      connection_configuration = {hosts: config['host'], connect_timeout: 120}
      connection_configuration[:compression] = config['compression'].to_sym if config['compression']
      @@connection ||= Cassandra.cluster(connection_configuration)
    end
  end
end