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

    def retype(column)
      raise "Cannot default unknown column #{column}" unless partition_key.include?(column)
      ColumnType.new(column, self)
    end

    private

    ColumnDefault = Struct.new(:column, :inquirer) do
      def to(value)
        default_to(value)

        case value
          when Integer then retype_to(:int)
          when Float then retype_to(:double)
          when Time then retype_to(:timestamp)
        end
      end

      def default_to(value)
        inquirer.column_defaults[column] = value
      end

      private

      def retype_to(type)
        ColumnType.new(column, inquirer).retype_to(type)
      end
    end

    ColumnType = Struct.new(:column, :inquirer) do
      def to(type)
        retype_to(type)
      end

      def retype_to(type)
        inquirer.partition_key[column] = type
      end
    end

  end
end