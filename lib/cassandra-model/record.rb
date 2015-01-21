class Record
  DEFAULT_CONFIGURATION = {
      :hosts => ['localhost'],
      keyspace: 'default_keyspace',
      port: '9042'
  }

  attr_reader :attributes

  def initialize(attributes)
    attributes.keys.each { |key| raise "Invalid column '#{key}' specified" unless self.class.columns.include?(key) }
    @attributes = attributes
  end

  @@statement_cache = {}
  class << self
    attr_reader :columns

    def table_name=(value)
      @table_name = value
    end

    def table_name
      @table_name ||= self.to_s.underscore.pluralize
    end

    def primary_key=(values)
      if values.is_a?(Array)
        partition_key = values.shift
        partition_key = [partition_key] unless partition_key.is_a?(Array)
        @primary_key = [partition_key, *values]
      else
        @primary_key = [[values]]
      end
    end

    def primary_key
      @primary_key
    end

    def columns=(values)
      @columns = values
      @columns.each do |column|
        define_method(:"#{column}=") { |value| @attributes[column] = value }
        define_method(column.to_sym) { @attributes[column] }
      end
    end

    def config=(value)
      @@config = DEFAULT_CONFIGURATION.merge(value)
    end

    def config
      @@config ||= DEFAULT_CONFIGURATION
    end

    def cluster
      connection_configuration = {hosts: config[:hosts], connect_timeout: 120}
      connection_configuration[:compression] = config[:compression].to_sym if config[:compression]
      @@connection ||= Cassandra.cluster(connection_configuration)
    end

    def connection
      cluster.connect(config[:keyspace])
    end

    def statement(query)
      @@statement_cache[query] ||= connection.prepare(query)
    end

    def where(clause)
      where_clause = if clause.size > 0
                       " WHERE #{clause.map { |key, _| "#{key} = ?" }.join(' AND ') }"
                     end
      results = connection.execute(statement("SELECT * FROM #{table_name}#{where_clause}"), *clause.values)
      results.map do |row|
        attributes = row.symbolize_keys
        Record.new(attributes)
      end
    end

    def paginate(*args)
      page = connection.execute(*args)
      while page
        yield page
        break if page.last_page?
        page = page.next_page
      end
    end
  end
end