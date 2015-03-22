module CassandraModel
  class TableDefinition
    def initialize(options)
      @partition_key = options[:partition_key]
      @clustering_columns = options[:clustering_columns]
      @name = options[:name]
      @columns = options[:columns]
    end

    def to_cql
      columns = @columns.map { |name, type| "#{name} #{type}" } * ', '
      "CREATE TABLE #{@name} (#{columns}, PRIMARY KEY ((#{@partition_key * ', '}), #{@clustering_columns * ', '}))"
    end
  end
end