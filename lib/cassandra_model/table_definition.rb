module CassandraModel
  class TableDefinition
    attr_reader :name

    def self.from_data_model(name, inquirer, data_set)
      partition_key = inquirer_partition_key(inquirer)
      partition_key.merge!(rk_shard: :int) if inquirer.is_sharding
      clustering_columns = table_set_clustering_columns(data_set)
      remaining_columns = table_set_remaining_columns(data_set)
      new(name: name, partition_key: partition_key,
          clustering_columns: clustering_columns,
          remaining_columns: remaining_columns)
    end

    def initialize(options)
      @partition_key = options[:partition_key].keys
      @clustering_columns = options[:clustering_columns].keys
      @name = options[:name]
      @columns = options[:partition_key].merge(options[:clustering_columns].merge(options[:remaining_columns]))
    end

    def to_cql(options = {})
      table_name = options[:no_id] ? name : name_in_cassandra
      exists = if options[:check_exists]
                 'IF NOT EXISTS '
               end
      "CREATE TABLE #{exists}#{table_name} (#{columns}, PRIMARY KEY #{primary_key})"
    end

    def table_id
      Digest::MD5.hexdigest(columns)
    end

    def name_in_cassandra
      "#{name}_#{table_id}"
    end

    def ==(rhs)
      to_cql == rhs.to_cql
    end

    private

    def self.table_set_remaining_columns(data_set)
      data_set.columns.except(*data_set.clustering_columns)
    end

    def self.table_set_clustering_columns(data_set)
      data_set.clustering_columns.inject({}) do |memo, column|
        memo.merge!(:"ck_#{column}" => data_set.columns[column])
      end
    end

    def self.inquirer_partition_key(inquirer)
      inquirer.partition_key.inject({}) do |memo, (key, value)|
        memo.merge!(:"rk_#{key}" => value)
      end
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
