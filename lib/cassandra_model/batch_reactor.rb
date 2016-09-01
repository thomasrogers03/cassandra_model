module CassandraModel
  class BatchReactor < ::BatchReactor::ReactorCluster
    include ThomasUtils::PerformanceMonitorMixin
    extend Forwardable

    def_delegator :@session, :prepare

    def initialize(cluster, session, batch_klass, options)
      @cluster = cluster
      @session = session
      @batch_klass = batch_klass

      define_partitioner(&method(:partition))
      super(cluster.hosts.count, options, &method(:batch_callback))
    end

    private

    def partition(statement)
      hosts = @cluster.find_replicas(@session.keyspace, statement)
      @cluster.hosts.find_index(hosts.first) || 0
    end

    def batch_callback(_)
      batch = @batch_klass.new
      yield batch
      future = @session.execute_async(batch).then { |result| batch.result = result }
      monitor_performance(__method__, {name: :batching, size: batch.statements.count}, Observable.create_observation(future))
    end

  end
end
