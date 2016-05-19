module CassandraModel
  class ResultReducerByKeys
    include Enumerable

    def initialize(enum, keys)
      @enum = enum
      @keys = keys
    end

    def each(&block)
      return to_enum(:each) unless block_given?

      if @keys.any?
        seen = Set.new

        @enum.each do |row|
          row_key = @keys.map { |column| row.public_send(column) }
          unless seen.include?(row_key)
            yield row
            seen << row_key
          end
        end
      else
        @enum.each(&block)
      end
    end

  end
end
