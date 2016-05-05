module CassandraModel
  class ResultChunker

    def initialize(enum, cluster)
      @enum = enum
      @cluster = cluster
    end

    def each(&block)
      @enum.chunk do |value|
        value.attributes.values_at(*@cluster)
      end.each(&block)
    end

  end
end
