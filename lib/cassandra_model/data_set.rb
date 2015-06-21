module CassandraModel
  class DataSet
    attr_reader :columns, :clustering_columns

    def initialize
      @columns = []
    end

    def knows_about(*columns)
      @columns |= columns
    end

    def is_defined_by(*columns)
      knows_about(*columns)
      @clustering_columns = columns
    end
  end
end