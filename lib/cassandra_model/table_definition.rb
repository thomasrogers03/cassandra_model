module CassandraModel
  class TableDefinition
    attr_reader :name

    class << self

      def from_data_model(table_name, inquirer, data_set)
        partition_key = inquirer_partition_key(inquirer)
        if inquirer.shard_column
          if inquirer.shard_column.is_a?(Hash)
            column_name, type = inquirer.shard_column.first
            partition_key.merge!(:"rk_#{column_name}" => type)
          else
            partition_key.merge!(:"rk_#{inquirer.shard_column}" => :int)
          end
        end
        clustering_columns = table_set_clustering_columns(data_set)
        remaining_columns = table_set_remaining_columns(data_set)
        new(name: table_name, partition_key: partition_key,
            clustering_columns: clustering_columns,
            remaining_columns: remaining_columns)
      end

      private

      def table_set_remaining_columns(data_set)
        data_set.columns.except(*data_set.clustering_columns)
      end

      def table_set_clustering_columns(data_set)
        data_set.clustering_columns.inject({}) do |memo, column|
          memo.merge!(:"ck_#{column}" => data_set.columns[column])
        end
      end

      def inquirer_partition_key(inquirer)
        inquirer.partition_key.inject({}) do |memo, (key, value)|
          memo.merge!(:"rk_#{key}" => value)
        end
      end

    end

    attr_reader :table_id, :name_in_cassandra

    def initialize(options)
      @partition_key = options[:partition_key].keys
      @clustering_columns = options[:clustering_columns].keys
      @name = options[:name]
      @columns = options[:partition_key].merge(options[:clustering_columns].merge(options[:remaining_columns]))
      @table_id = generate_table_id
      @name_in_cassandra = "#{name}_#{table_id}"
      @properties = options[:properties] || {}
    end

    def to_cql(options = {})
      table_name = options[:no_id] ? name : name_in_cassandra
      exists = if options[:check_exists]
                 'IF NOT EXISTS '
               end
      properties = if @properties.present?
                     property_values = @properties.map do |property, definition|
                       case property
                         when :compaction
                           "COMPACTION = #{to_property_string(definition)}"
                         when :clustering_order
                           "CLUSTERING ORDER BY #{to_clustering_order_string(definition)}"
                       end
                     end * ' AND '
                     " WITH #{property_values}"
                   end
      "CREATE TABLE #{exists}#{table_name} (#{columns}, PRIMARY KEY #{primary_key})#{properties}"
    end

    def ==(rhs)
      to_cql == rhs.to_cql
    end

    private

    def to_property_string(property)
      "{#{property.map { |key, value| "'#{key}': '#{value}'" } * ', '}}"
    end

    def to_clustering_order_string(clustering_order)
      "(#{clustering_order.map { |column, order| "#{column} #{order.upcase}" } * ', '})"
    end

    def generate_table_id
      Digest::MD5.hexdigest(columns)
    end

    def columns
      @columns.map { |name, type| "#{name} #{type}" } * ', '
    end

    def primary_key
      if @clustering_columns.present?
        "((#{@partition_key * ', '}), #{@clustering_columns * ', '})"
      else
        "((#{@partition_key * ', '}))"
      end
    end
  end
end
