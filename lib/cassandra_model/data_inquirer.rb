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
      raise "Cannot default unknown column #{column}" unless partition_key.include?(column)
      ColumnDefault.new(column, self)
    end

    private

    ColumnDefault = Struct.new(:column, :inquirer) do
      def to(value)
        inquirer.column_defaults[column] = value

        case value
          when Integer then inquirer.partition_key[column] = :int
          when Float then inquirer.partition_key[column] = :double
        end
      end
    end

  end
end