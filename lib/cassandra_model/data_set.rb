module CassandraModel
  class DataSet
    attr_reader :columns, :clustering_columns

    def initialize
      @columns = Hash.new { |hash, key| hash[key] = :text }
    end

    def knows_about(*columns)
      columns.each { |column| @columns[column] }
    end

    def is_defined_by(*columns)
      knows_about(*columns)
      @clustering_columns = columns
    end

    def retype(column)
      raise "Cannot retype unknown column #{column}" unless columns.include?(column)
      ColumnType.new(column, self)
    end

    private

    ColumnType = Struct.new(:column, :inquirer) do
      def to(type)
        inquirer.columns[column] = type
      end
    end

  end
end