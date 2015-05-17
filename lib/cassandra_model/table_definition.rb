module CassandraModel
  class TableDefinition
    attr_reader :name

    def initialize(options)
      @partition_key = options[:partition_key].keys
      @clustering_columns = options[:clustering_columns].keys
      @name = options[:name]
      @columns = options[:partition_key].merge(options[:clustering_columns].merge(options[:remaining_columns]))
    end

    def to_cql(options = {})
      table_name = options[:no_id] ? name : name_in_cassandra
      "CREATE TABLE #{table_name} (#{columns}, PRIMARY KEY (#{primary_key})"
    end

    def table_id
      Digest::MD5.hexdigest(columns)
    end

    def name_in_cassandra
      "#{name}_#{table_id}"
    end

    private

    def columns
      @columns.map { |name, type| "#{name} #{type}" } * ', '
    end

    def primary_key
      "(#{@partition_key * ', '}), #{@clustering_columns * ', '})"
    end
  end
end