module CassandraModel
  class RawConnection
    def cluster
      @cluster ||= begin
        Cassandra::Mocks::Cluster.new.tap do |cluster|
          cluster.add_keyspace(config[:keyspace])
        end
      end
    end
  end
end
