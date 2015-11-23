module CassandraModel
  class DataInquirer
    attr_reader :partition_key, :column_defaults, :is_sharding

    def initialize
      @partition_key = Hash.new { |hash, key| hash[key] = :text }
      @column_defaults = Hash.new { |hash, key| hash[key] = '' }
      @known_keys = []
    end

    def guess_data_types!
      @guess_data_types = true
    end

    def knows_about(*columns)
      columns.each do |column|
        if @guess_data_types
          guess_data_type(column)
        else
          partition_key[column]
        end
        column_defaults[column]
      end
      @known_keys << columns
      self
    end

    def shards_queries
      @is_sharding = true
    end

    def composite_rows
      @known_keys.map do |row|
        partition_key.keys - row
      end.reject(&:empty?)
    end

    def defaults(column)
      raise "Cannot default unknown column #{column}" unless partition_key.include?(column)
      ColumnDefault.new(column, self)
    end

    def change_type_of(column)
      raise "Cannot retype unknown column #{column}" unless partition_key.include?(column)
      ColumnType.new(column, self)
    end

    private

    ColumnDefault = Struct.new(:column, :inquirer) do
      def to(value)
        default_to(value)

        case value
          when Integer then
            retype_to(:int)
          when Float then
            retype_to(:double)
          when Time then
            retype_to(:timestamp)
          when Cassandra::Uuid then
            retype_to(:uuid)
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

        case type
          when :int then
            default_to(0)
          when :double then
            default_to(0.0)
          when :timestamp then
            default_to(Time.at(0))
          when :uuid then
            default_to(Cassandra::Uuid.new(0))
        end
      end

      def retype_to(type)
        inquirer.partition_key[column] = type
      end

      private

      def default_to(value)
        ColumnDefault.new(column, inquirer).default_to(value)
      end
    end

    class DataTypeGuess < Struct.new(:column)
      def guessed_type
        postfix_type || :text
      end

      private

      def postfix_type
        if column =~ /_at$/
          :timestamp
        elsif column =~ /_at_id$/
          :timeuuid
        elsif column =~ /_id$/
          :uuid
        elsif column =~ /_(price|average|stddev)$/
          :double
        elsif column =~ /_(year|day|month|index|total|count)$/
          :int
        elsif column =~ /_data/
          :blob
        elsif column =~ /_map$/
          'map<string, string>'
        end
      end
    end

    def guess_data_type(column)
      partition_key[column] = DataTypeGuess.new(column).guessed_type
    end

  end
end
