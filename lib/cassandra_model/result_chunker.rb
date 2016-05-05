module CassandraModel
  class ResultChunker
    include Enumerable

    def initialize(enum, cluster)
      @enum = enum
      @cluster = cluster
    end

    def each(&block)
      @enum.chunk do |value|
        value.attributes.values_at(*@cluster)
      end.each(&block)
    end

    alias :get :to_a

  end
end
