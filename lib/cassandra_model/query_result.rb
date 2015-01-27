module CassandraModel
  class QueryResult
    @@class_cache = {}

    attr_reader :attributes

    def initialize(attributes)
      @attributes = attributes
    end

    def self.create(attributes)
      columns = attributes.keys
      klass = (@@class_cache[columns] ||= Class.new(QueryResult))
      result = klass.new(attributes)
      columns.each { |column| klass.send(:define_method, column.to_sym) { self.attributes[column] } }
      result
    end

    def ==(rhs)
      attributes == rhs.attributes
    end
  end
end