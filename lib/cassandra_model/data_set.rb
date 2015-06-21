module CassandraModel
  class DataSet
    attr_reader :columns

    def initialize
      @columns = []
    end

    def knows_about(*columns)
      @columns |= columns
    end
  end
end