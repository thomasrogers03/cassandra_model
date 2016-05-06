module CassandraModel
  class ResultReducer
    include Enumerable

    def initialize(enum, filter_keys)
      @enum = enum
      @filter_keys = filter_keys
    end

    def each(&block)
      @enum.each do |_, rows|
        if @filter_keys.one?
          yield rows.first
        elsif @filter_keys.any?
          prev_filter = []
          row_iterator = rows.each

          @filter_keys.length.times do |index|
            row = row_iterator.next
            next_filter = row_filter(row, index)
            break unless next_filter == prev_filter
            prev_filter = next_filter << row_filter_key(index, row)
            yield row
          end
        else
          rows.each(&block)
        end
      end
    end

    private

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
