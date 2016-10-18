module CassandraModel
  class ResultChunker
    include Enumerable
    include ThomasUtils::Enum::Indexing

    def initialize(enum, cluster)
      @enum = enum
      @cluster = cluster
    end

    def each(&block)
      enum.chunk do |value|
        value.attributes.values_at(*cluster)
      end.each(&block)
    end

    alias :get :to_a

    def ==(rhs)
      rhs.is_a?(ResultChunker) &&
          enum == rhs.enum &&
          cluster == rhs.cluster
    end

    protected

    attr_reader :enum, :cluster

  end
end
