module CassandraModel
  class ResultLimiter
    include Enumerable

    def initialize(enum, limit)
      @enum = enum
      @limit = limit
    end

    def each
      return to_enum(:each) unless block_given?

      @enum.each.with_index do |value, index|
        break if index >= @limit
        yield value
      end
    end

    alias :get :to_a
  end
end