module CassandraModel
  class RawConnection
    DEFAULT_CONFIGURATION = {
        hosts: %w(localhost),
        keyspace: 'default_keyspace',
        port: '9042'
    }.freeze

    def initialize(config_name = nil)
      @config_name = config_name
      @statement_cache = {}
    end

    def config=(value)
      @config = DEFAULT_CONFIGURATION.merge(value)
    end

    def config
      @config ||= load_config
    end

    def cluster
      @cluster ||= begin
        connection_configuration = {hosts: config[:hosts], connect_timeout: 120, logger: Logging.logger}
        connection_configuration[:compression] = config[:compression].to_sym if config[:compression]
        Cassandra.cluster(connection_configuration)
      end
    end

    def session
      @session ||= cluster.connect(config[:keyspace])
    end

    def keyspace
      cluster.keyspace(config[:keyspace])
    end

    def statement(query)
      @statement_cache[query] ||= session.prepare(query)
    end

    private

    def statement_cache
      @statement_cache ||= {}
    end

    def load_config
      if File.exists?(config_path)
        config = yaml_config || {}
        DEFAULT_CONFIGURATION.merge(config)
      else
        DEFAULT_CONFIGURATION
      end
    end

    def yaml_config
      yaml_config = File.open(config_path) { |file| YAML.load(file.read) }
      yaml_config = yaml_config[Rails.env] if defined?(Rails)
      yaml_config
    end

    def config_path
      @config_name ? "./config/cassandra/#{@config_name}.yml" : './config/cassandra.yml'
    end
  end
end