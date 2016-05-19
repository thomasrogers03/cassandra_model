module CassandraModel
  class ResultFilter
    include Enumerable

    def initialize(enum, &filter)
      @enum = enum
      @filter = filter
    end

    def each(&block)
      return to_enum(:each) unless block_given?

      @enum.each do |value|
        block[value] if @filter[value]
      end
    end

  end
end
