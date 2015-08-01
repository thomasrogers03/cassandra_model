module CassandraModel
  class RawConnection
    CLUSTER_MUTEX = Mutex.new
    SESSION_MUTEX = Mutex.new
    CONFIG_MUTEX = Mutex.new
    STATEMENT_MUTEX = Mutex.new

    DEFAULT_CONFIGURATION = {
        hosts: %w(localhost),
        keyspace: 'default_keyspace',
        port: '9042',
        consistency: :one,
        connection_timeout: 10,
        timeout: 10
    }.freeze

    def initialize(config_name = nil)
      @config_name = config_name
      @statement_cache = {}
    end

    def config=(value)
      CONFIG_MUTEX.synchronize { @config = DEFAULT_CONFIGURATION.merge(value) }
    end

    def config
      safe_getset_variable(CONFIG_MUTEX, :@config) { load_config }
    end

    def cluster
      safe_getset_variable(CLUSTER_MUTEX, :@cluster) do
        connection_configuration = config.slice(:hosts,
                                                :compression,
                                                :consistency,
                                                :connection_timeout, :timeout,
                                                :username, :password,
                                                :address_resolution)
        connection_configuration.merge!(logger: Logging.logger)
        Cassandra.cluster(connection_configuration)
      end
    end

    def session
      safe_getset_variable(SESSION_MUTEX, :@session) { cluster.connect(config[:keyspace]) }
    end

    def keyspace
      cluster.keyspace(config[:keyspace])
    end

    def statement(query)
      statement_cache[query] || begin
        STATEMENT_MUTEX.synchronize { statement_cache[query] ||= session.prepare(query) }
      end
    end

    private

    attr_reader :statement_cache

    def safe_getset_variable(mutex, name, &block)
      result = instance_variable_get(name)
      return result if result

      mutex.synchronize do
        result = instance_variable_get(name)
        return result if result

        instance_variable_set(name, block.call)
      end
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