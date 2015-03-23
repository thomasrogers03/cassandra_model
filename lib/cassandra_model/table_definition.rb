module CassandraModel
  class TableDefinition
    attr_reader :name

    def initialize(options)
      @partition_key = options[:partition_key]
      @clustering_columns = options[:clustering_columns]
      @name = options[:name]
      @columns = options[:columns]
    end

    def to_cql
      "CREATE TABLE #{@name} (#{columns}, PRIMARY KEY (#{primary_key})"
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