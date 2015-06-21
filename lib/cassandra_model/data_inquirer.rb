module CassandraModel
  class DataInquirer
    attr_reader :partition_key
    attr_reader :column_defaults

    def initialize
      @partition_key = Hash.new { |hash, key| hash[key] = :string }
      @column_defaults = Hash.new { |hash, key| hash[key] = '' }
    end

    def knows_about(*columns)
      columns.each do |column|
        partition_key[column]
        column_defaults[column]
      end
      self
    end

    def defaults(column)
      ColumnDefault.new(column, column_defaults)
    end

    private

    ColumnDefault = Struct.new(:column, :column_defaults) do
      def to(value)
        column_defaults[column] = value
      end
    end

  end
end