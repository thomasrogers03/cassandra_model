module CassandraModel
  class BatchReactor < ::BatchReactor::ReactorCluster

    def initialize(cluster, keyspace, batch_klass, options)
      @cluster = cluster
      @keyspace = keyspace
      @batch_klass = batch_klass

      define_partitioner(&method(:partition))
      super(cluster.hosts.count, options, &method(:batch_callback))
    end

    private

    def partition(statement)
      hosts = @cluster.find_replicas(@keyspace, statement)
      @cluster.hosts.find_index(hosts.first) || 0
    end

    def batch_callback(_)
      yield @batch_klass.new
      Ione::Future.resolved(true)
    end

  end
end
