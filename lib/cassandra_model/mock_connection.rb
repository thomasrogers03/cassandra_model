module CassandraModel
  class RawConnection
    def cluster
      @cluster ||= Cassandra::Mocks::Cluster.new
    end
  end
end
