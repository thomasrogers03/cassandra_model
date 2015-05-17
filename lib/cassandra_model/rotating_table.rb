module CassandraModel
  class RotatingTable
    extend CassandraModel::Connection

    def initialize(tables, schedule)
      @tables = tables
      @schedule = schedule
    end

    def name
      index = (Time.now.to_f / @schedule).to_i % @tables.count
      @tables[index].name
    end
  end
end