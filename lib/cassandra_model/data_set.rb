module CassandraModel
  class DataSet
    attr_reader :columns, :clustering_columns

    def initialize
      @columns = Hash.new { |hash, key| hash[key] = :string }
    end

    def knows_about(*columns)
      columns.each { |column| @columns[column] }
    end

    def is_defined_by(*columns)
      knows_about(*columns)
      @clustering_columns = columns
    end
  end
end