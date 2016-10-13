module CassandraModel
  class ResultReducer
    include Enumerable

    def initialize(enum, filter_keys)
      @enum = enum
      @filter_keys = filter_keys
    end

    def each(&block)
      return self unless block_given?

      @enum.each do |*_, rows|
        if @filter_keys.one?
          yield rows.first
        elsif @filter_keys.any?
          filter_results(rows, &block)
        else
          rows.each(&block)
        end
      end
    end

    private

    def filter_results(rows)
      prev_filter = []

      rows.each.with_index do |row, index|
        break if index >= @filter_keys.length
        next_filter = row_filter(row, index)
        break unless next_filter == prev_filter
        prev_filter = next_filter << row_filter_key(index, row)
        yield row
      end
    end

    def row_filter(row, filter_length)
      filter_length.times.map do |index|
        row_filter_key(index, row)
      end
    end

    def row_filter_key(index, row)
      row.attributes[@filter_keys[index]]
    end

  end
end
