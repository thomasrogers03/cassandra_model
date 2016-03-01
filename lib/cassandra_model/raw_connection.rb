module CassandraModel
  #noinspection RubyTooManyInstanceVariablesInspection
  class RawConnection
    extend Forwardable

    DEFAULT_CONFIGURATION = {
        hosts: %w(localhost),
        keyspace: 'default_keyspace',
        keyspace_options: {
            class: 'SimpleStrategy',
            replication_factor: 1
        },
        port: '9042',
        consistency: :one,
        connection_timeout: 10,
        timeout: 10
    }.freeze

    include ConcurrencyHelper

    def_delegator :@executor, :value, :executor
    def_delegator :@futures_factory, :value, :futures_factory

    def initialize(config_name = nil)
      @config_name = config_name
      @statement_cache = Concurrent::Map.new

      @cluster_mutex = Mutex.new
      @session_mutex = Mutex.new
      @config_mutex = Mutex.new
      @reactor_mutex = Mutex.new

      @executor = Concurrent::Delay.new { Concurrent::CachedThreadPool.new }
      @futures_factory = Concurrent::Delay.new { Cassandra::Future::Factory.new(executor) }
    end

    def config=(value)
      @config_mutex.synchronize { @config = DEFAULT_CONFIGURATION.merge(value) }
    end

    def config
      safe_getset_variable(@config_mutex, :@config) { load_config }
    end

    def cluster
      safe_getset_variable(@cluster_mutex, :@cluster) do
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
      safe_getset_variable(@session_mutex, :@session) { cluster.connect(config[:keyspace]) }
    end

    def keyspace
      cluster.keyspace(keyspace_name) || create_keyspace
    end

    def unlogged_batch_reactor
      reactor(:@unlogged_reactor, SingleTokenUnloggedBatch)
    end

    def logged_batch_reactor
      reactor(:@logged_reactor, SingleTokenLoggedBatch)
    end

    def counter_batch_reactor
      reactor(:@counter_reactor, SingleTokenCounterBatch)
    end

    def statement(query)
      statement_cache.fetch_or_store(query) { session.prepare(query) }
    end

    def shutdown
      @shutdown = true
      @reactor_mutex.synchronize do
        @unlogged_reactor.stop.get if @unlogged_reactor
        @unlogged_reactor = nil

        @logged_reactor.stop.get if @logged_reactor
        @logged_reactor = nil

        @counter_reactor.stop.get if @counter_reactor
        @counter_reactor = nil
      end
      @session_mutex.synchronize do
        @session.close if @session
        @session = nil
      end
      @cluster_mutex.synchronize do
        @cluster.close if @cluster
        @cluster = nil
      end
    end

    private

    attr_reader :statement_cache

    def create_keyspace
      cluster.connect.execute(create_keyspace_query)
      sleep 0.1 until (keyspace = cluster.keyspace(keyspace_name))
      keyspace
    end

    def create_keyspace_query
      "CREATE KEYSPACE IF NOT EXISTS #{keyspace_name} WITH REPLICATION = #{keyspace_options};"
    end

    def keyspace_options
      keyspace_options = config[:keyspace_options].map do |key, value|
        value = "'#{value}'" if value.is_a?(String)
        "'#{key}' : #{value}"
      end * ', '
      "{ #{keyspace_options} }"
    end

    def keyspace_name
      config[:keyspace]
    end

    def reactor(name, type)
      safe_getset_variable(@reactor_mutex, name) do
        BatchReactor.new(cluster, session, type, config[:batch_reactor] || {}).tap do |reactor|
          reactor.start.get
        end
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
