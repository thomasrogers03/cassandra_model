module CassandraModel
  class DataInquirer
    attr_reader :partition_key, :column_defaults

    def initialize
      @partition_key = Hash.new { |hash, key| hash[key] = :string }
      @column_defaults = Hash.new { |hash, key| hash[key] = '' }
      @known_keys = []
    end

    def knows_about(*columns)
      columns.each do |column|
        partition_key[column]
        column_defaults[column]
      end
      @known_keys << columns
      self
    end

    def composite_rows
      @known_keys.map do |row|
        partition_key.keys - row
      end
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