class Record
  DEFAULT_CONFIGURATION = {
      hosts: %w(localhost),
      keyspace: 'default_keyspace',
      port: '9042'
  }

  attr_reader :attributes

  def initialize(attributes)
    attributes.keys.each { |column| raise "Invalid column '#{column}' specified" unless columns.include?(column) }
    @attributes = attributes
  end

  def save_async
    future = save_row_async(column_values)
    FutureWrapper.new(future) { self }
  end

  def save
    save_async.get
  end

  def ==(rhs)
    @attributes == rhs.attributes
  end

  private

  def column_values
    columns.map { |column| attributes[column] }
  end

  def save_row_async(column_values)
    Record.connection.execute_async(Record.statement(query_for_save), *column_values)
  end

  def query_for_save
    self.class.query_for_save
  end

  def columns
    self.class.columns
  end

  @@connection = nil
  @@cluster = nil
  @@statement_cache = {}
  @@keyspace = nil
  class << self
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

    def config=(value)
      @@config = DEFAULT_CONFIGURATION.merge(value)
    end

    def config
      @@config ||= DEFAULT_CONFIGURATION
    end

    def cluster
      connection_configuration = {hosts: config[:hosts], connect_timeout: 120}
      connection_configuration[:compression] = config[:compression].to_sym if config[:compression]
      @@cluster ||= Cassandra.cluster(connection_configuration)
    end

    def connection
      @@connection ||= cluster.connect(config[:keyspace])
    end

    def keyspace
      unless @@keyspace
        connection
        @@keyspace = cluster.keyspace(config[:keyspace])
      end
      @@keyspace
    end

    def partition_key
      @partition_key ||= keyspace.table(table_name.to_s).send(:partition_key).map { |column| column.name.to_sym }
    end

    def clustering_columns
      @clustering_columns ||= keyspace.table(table_name.to_s).send(:clustering_columns).map { |column| column.name.to_sym }
    end

    def columns
      unless @columns
        @columns = keyspace.table(table_name.to_s).columns.map { |column| column.name.to_sym }
        @columns.each { |column| define_attribute(column) }
      end
      @columns
    end

    def statement(query)
      @@statement_cache[query] ||= connection.prepare(query)
    end

    def query_for_save
      column_names = columns.join(', ')
      column_sanitizers = (%w(?) * columns.size).join(', ')
      @save_query ||= "INSERT INTO #{table_name} (#{column_names}) VALUES (#{column_sanitizers})"
    end

    def create_async(attributes)
      self.new(attributes).save_async
    end

    def create(attributes)
      create_async(attributes).get
    end

    def where_async(clause)
      select_clause, use_query_result = select_params(clause)
      page_size = clause.delete(:page_size)
      limit_clause = limit_clause(clause)
      where_clause, where_values = where_params(clause)
      statement = statement("SELECT #{select_clause} FROM #{table_name}#{where_clause}#{limit_clause}")

      if page_size
        future = connection.execute_async(statement, *where_values, page_size: page_size)
        ResultPaginator.new(future) { |row| record_from_result(row, use_query_result) }
      else
        future = connection.execute_async(statement, *where_values)
        FutureWrapper.new(future) { |results| result_records(results, use_query_result) }
      end
    end

    def first_async(clause = {})
      FutureWrapper.new(where_async(clause.merge(limit: 1))) { |results| results.first }
    end

    def where(clause)
      page_size = clause[:page_size]
      future = where_async(clause)
      page_size ? future : future.get
    end

    def first(clause = {})
      first_async(clause).get
    end

    def paginate(*args)
      page = connection.execute(*args)
      while page
        yield page
        break if page.last_page?
        page = page.next_page
      end
    end

    private

    def define_attribute(column)
      define_method(:"#{column}=") { |value| self.attributes[column] = value }
      define_method(column.to_sym) { self.attributes[column] }
    end

    def limit_clause(clause)
      limit = clause.delete(:limit)
      if limit
        integer_limit = limit.to_i
        raise "Invalid limit '#{limit}'" if integer_limit < 1
        " LIMIT #{integer_limit}"
      end
    end

    def select_params(clause)
      select = clause.delete(:select)
      [select_clause(select), !!select]
    end

    def select_clause(select)
      select ? multi_select_clause(select) : '*'
    end

    def multi_select_clause(select)
      select.is_a?(Array) ? select.join(', ') : select
    end

    def where_params(clause)
      where_clause = where_clause(clause) if clause.size > 0
      where_values = *clause.values
      [where_clause, where_values]
    end

    def where_clause(clause)
      restriction = clause.keys.map { |key| "#{key} = ?" }.join(' AND ')
      " WHERE #{restriction}"
    end

    def result_records(results, use_query_result)
      results.map { |row| record_from_result(row, use_query_result) }
    end

    def record_from_result(row, use_query_result)
      attributes = row.symbolize_keys
      use_query_result ? QueryResult.create(attributes) : self.new(attributes)
    end

  end
end