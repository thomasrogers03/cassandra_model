module CassandraModel
  class RotatingTable
    extend Forwardable
    include TableDebug

    def_delegators :first_table, :primary_key, :partition_key, :clustering_columns, :columns
    def_delegators :table, :connection, :name, :truncate!

    def initialize(tables, schedule)
      columns = tables.first.columns
      raise 'RotatingTable, Table columns do not match' unless valid_tables?(columns, tables)

      @tables = tables
      @schedule = schedule
    end

    def allow_truncation!
      @allow_truncation = true
      tables.each(&:allow_truncation!)
    end

    def reset_local_schema!
      @tables.reject { |table| table.is_a?(MetaTable) }.each(&:reset_local_schema!)
    end

    def ==(rhs)
      @schedule == rhs.schedule &&
          @tables == rhs.tables
    end

    protected

    attr_reader :schedule, :tables

    private

    def first_table
      @tables.first
    end

    def table
      index = (Time.now.to_f / @schedule).to_i % @tables.count
      @tables[index]
    end

    def valid_tables?(columns, tables)
      tables.map(&:columns).reduce(&:|) == columns
    end

  end
end
