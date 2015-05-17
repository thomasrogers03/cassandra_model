module CassandraModel
  class MetaTable < Table
    def initialize(table_definition)
      @table_definition = table_definition
    end

    def name
      @table_definition.name.to_s
    end
  end
end