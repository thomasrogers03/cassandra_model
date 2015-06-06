module CassandraModel
  class RotatingTable
    extend Forwardable

    def_delegators :first_table, :partition_key, :clustering_columns, :columns

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

    def first_table
      @tables.first
    end

    def valid_tables?(columns, tables)
      tables.map(&:columns).reduce(&:|) == columns
    end

  end
end