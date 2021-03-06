module CassandraModel
  class DataInquirer
    include TypeGuessing

    attr_reader :partition_key, :column_defaults, :shard_column

    def initialize
      @partition_key = Hash.new { |hash, key| hash[key] = :text }
      @column_defaults = Hash.new { |hash, key| hash[key] = '' }
      @known_keys = []
    end

    def knows_about(*columns)
      columns.each do |column|
        partition_key[column]
        guess_data_type(column) if @guess_data_types
        column_defaults[column]
      end
      @known_keys << columns
      self
    end

    def shards_queries(column = :shard)
      @shard_column = column
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
          when String then
            retype_to(:text)
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
          when :text then
            default_to('')
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

    def guess_data_type(column)
      type = guessed_data_type(column, :int)
      change_type_of(column).to(type)
    end

  end
end
