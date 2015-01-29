module CassandraModel
  module Connection
    DEFAULT_CONFIGURATION = {
        hosts: %w(localhost),
        keyspace: 'default_keyspace',
        port: '9042'
    }

    @@config = nil
    @@connection = nil
    @@cluster = nil
    @@statement_cache = {}
    @@keyspace = nil

    def config=(value)
      @@config = DEFAULT_CONFIGURATION.merge(value)
    end

    def config
      unless @@config
        @@config = load_config()
      end
      @@config
    end

    def cluster
      connection_configuration = {hosts: config[:hosts], connect_timeout: 120}
      connection_configuration[:compression] = config[:compression].to_sym if config[:compression]
      @@cluster ||= Cassandra.cluster(connection_configuration)
    end

    def connection
      @@connection ||= cluster.connect(config[:keyspace])
    end

    def keyspace
      unless @@keyspace
        connection
        @@keyspace = cluster.keyspace(config[:keyspace])
      end
      @@keyspace
    end

    def statement(query)
      @@statement_cache[query] ||= connection.prepare(query)
    end

    private

    def load_config
      if File.exists?('./config/cassandra.yml')
        yaml_config = yaml_config()
        DEFAULT_CONFIGURATION.merge(yaml_config)
      else
        DEFAULT_CONFIGURATION
      end
    end

    def yaml_config
      yaml_config = File.open('./config/cassandra.yml') { |file| YAML.load(file.read) }
      yaml_config = yaml_config[Rails.env] if defined?(Rails)
      yaml_config.symbolize_keys
    end
  end
end