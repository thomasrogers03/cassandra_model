module CassandraModel
  class RotatingTable
    def initialize(tables, schedule)
      columns = tables.first.columns
      raise 'RotatingTable, Table columns do not match' unless valid_tables?(columns, tables)

      @tables = tables
      @schedule = schedule
    end

    def name
      index = (Time.now.to_f / @schedule).to_i % @tables.count
      @tables[index].name
    end

    private

    def valid_tables?(columns, tables)
      tables.map(&:columns).reduce(&:|) == columns
    end

  end
end