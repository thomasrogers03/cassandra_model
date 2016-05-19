module CassandraModel
  class ResultFilter
    include Enumerable

    def initialize(enum, &filter)
      @enum = enum
      @filter = filter
    end

    def each
      return to_enum(:each) unless block_given?

      enum.each do |*_, value|
        yield value if filter[value]
      end
    end

    def ==(rhs)
      rhs.is_a?(ResultFilter) &&
          rhs.filter == filter &&
          rhs.enum == enum
    end

    protected

    attr_reader :enum, :filter

  end
end
