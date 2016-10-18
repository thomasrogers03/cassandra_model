module CassandraModel
  class ResultReducerByKeys
    include Enumerable
    include ThomasUtils::Enum::Indexing

    def initialize(enum, keys)
      @enum = enum
      @keys = keys
    end

    def each(&block)
      return self unless block_given?

      if keys.any?
        seen = Set.new

        enum.each do |row|
          row_key = keys.map { |column| row.public_send(column) }
          unless seen.include?(row_key)
            yield row
            seen << row_key
          end
        end
      else
        enum.each(&block)
      end
    end

    def ==(rhs)
      rhs.is_a?(ResultReducerByKeys) &&
          rhs.enum == enum &&
          rhs.keys == keys
    end

    protected

    attr_reader :enum, :keys

  end
end
