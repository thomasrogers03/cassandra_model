module CassandraModel
  module SingleTokenBatch
    extend Forwardable
    include Enumerable

    attr_writer :result

    def_delegators :result, :execution_info, :empty?, :each

    def keyspace
      nil
    end

    def partition_key
      @statements.first.partition_key
    end

    private

    attr_reader :result

  end
end
