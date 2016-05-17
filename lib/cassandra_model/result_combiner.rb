module CassandraModel
  class ResultCombiner
    include Enumerable

    def initialize(lhs, rhs)
      @lhs = lhs
      @rhs = rhs
    end

    def each(&block)
      return to_enum(:each) unless block_given?

      @lhs.each(&block)
      @rhs.each(&block)
    end

  end
end

