require_relative 'query_helper'
require_relative 'meta_columns'

module CassandraModel
  class Record
    extend CassandraModel::QueryHelper
    extend CassandraModel::MetaColumns

    attr_reader :attributes, :valid

    Attributes = Struct.new(
        :table_name,
        :connection_name,
        :table,

        :columns,
        :counter_columns,

        :before_save_callbacks,

        :deferred_column_readers,
        :deferred_column_writers,
        :async_deferred_column_readers,
        :async_deferred_column_writers,

        :internal_defaults,
        :composite_defaults,
        :composite_columns,
        :composite_pk_map,
        :composite_ck_map,
    ) # Using this instead of OpenStruct, as there seems to be a bug in JRuby that causes this to get mangled over time

    def initialize(attributes, options = {validate: true})
      validate_attributes!(attributes) if options[:validate]
      @valid = true
      @attributes = attributes
      self.class.after_initialize(self)
      self.class.before_save_callbacks.map { |proc| instance_eval(&proc) }
    end

    def save_async(options = {})
      internal_save_async(options)
    end

    def delete_async
      internal_delete_async
    end

    def update_async(new_attributes)
      internal_update_async(new_attributes)
    end

    def invalidate!
      @valid = false
    end

    def save(options = {})
      save_async(options).get
    end

    def delete
      delete_async.get
    end

    def update(new_attributes)
      update_async(new_attributes).get
    end

    def ==(rhs)
      @attributes == rhs.attributes
    end

    protected

    def table
      self.class.table
    end

    def session
      table.connection.session
    end

    def statement(query)
      table.connection.statement(query)
    end

    def validate_attributes!(attributes)
      attributes.keys.each do |column|
        column = column.key if column.is_a?(ThomasUtils::KeyIndexer)
        raise "Invalid column '#{column}' specified" unless columns.include?(column)
      end
    end

    def internal_delete_async
      @valid = false

      statement = statement(self.class.query_for_delete)
      attributes = internal_attributes
      column_values = (self.class.partition_key + self.class.clustering_columns).map { |column| attributes[column] }
      future = session.execute_async(statement, *column_values, {})
      ThomasUtils::FutureWrapper.new(future) { self }
    end

    def internal_save_async(options = {})
      raise 'Cannot save invalidated record!' unless valid

      if self.class.deferred_column_writers || self.class.async_deferred_column_writers
        ThomasUtils::Future.new do
          save_deferred_columns
          future = save_row_async(options)
          ThomasUtils::FutureWrapper.new(future) { self }
        end
      else
        future = save_row_async(options)
        ThomasUtils::FutureWrapper.new(future) do |result|
          invalidate! if save_rejected?(result)
          self
        end
      end
    end

    def save_rejected?(result)
      save_result = result.first
      save_result && save_result['[applied]'] == false
    end

    def internal_update_async(new_attributes)
      validate_attributes!(new_attributes)

      query = self.class.query_for_update(new_attributes)
      statement = statement(query)
      attributes = internal_attributes
      column_values = (self.class.partition_key + self.class.clustering_columns).map { |column| attributes[column] }
      future = session.execute_async(statement, *new_attributes.values, *column_values, {})
      ThomasUtils::FutureWrapper.new(future) do
        self.attributes.merge!(new_attributes)
        self
      end
    end

    def column_values
      attributes = internal_attributes
      internal_columns.map { |column| attributes[column] }
    end

    def internal_attributes
      attributes
    end

    def save_row_async(options)
      session.execute_async(statement(query_for_save(options)), *column_values, {})
    end

    def save_deferred_columns
      self.class.save_deferred_columns(self)
      deferred_column_futures = self.class.save_async_deferred_columns(self)
      deferred_column_futures.map(&:get) if deferred_column_futures
    end

    def query_for_save(options)
      self.class.query_for_save(options)
    end

    def columns
      self.class.columns
    end

    def internal_columns
      self.class.internal_columns
    end

    class << self
      extend Forwardable

      def_delegators :table, :partition_key, :clustering_columns
      def_delegator :table, :name, :table_name
      def_delegator :table, :columns, :internal_columns

      def table_name=(value)
        table_data.table_name = value
      end

      def connection_name=(value)
        table_data.connection_name = value
      end

      def table=(value)
        table_data.table = value
      end

      def table
        table_data.table ||= begin
          table_name = table_data.table_name || generate_table_name
          TableRedux.new(table_data.connection_name, table_name)
        end
      end

      def columns
        table_data.columns ||= internal_columns.tap do |columns|
          columns.each { |column| define_attribute(column) }
        end
      end

      def query_for_save(options = {})
        existence_clause = options[:check_exists] && ' IF NOT EXISTS'
        column_names = internal_columns.join(', ')
        column_sanitizers = (%w(?) * internal_columns.size).join(', ')
        save_query = "INSERT INTO #{table_name} (#{column_names}) VALUES (#{column_sanitizers})"
        "#{save_query}#{existence_clause}"
      end

      def query_for_delete
        where_clause = (partition_key + clustering_columns).map { |column| "#{column} = ?" }.join(' AND ')
        "DELETE FROM #{table_name} WHERE #{where_clause}"
      end

      def query_for_update(new_attributes)
        where_clause = (partition_key + clustering_columns).map { |column| "#{column} = ?" }.join(' AND ')
        set_clause = new_attributes.keys.map { |column| "#{column} = ?" }.join(' AND ')
        "UPDATE #{table_name} SET #{set_clause} WHERE #{where_clause}"
      end

      def create_async(attributes, options = {})
        self.new(attributes).save_async(options)
      end

      def create(attributes, options = {})
        create_async(attributes, options).get
      end

      def request_async(clause, options = {})
        page_size = options[:page_size]
        request_query, use_query_result, where_values = request_meta(clause, options)
        statement = statement(request_query)

        query_options = {}
        query_options[:page_size] = page_size if page_size

        future = session.execute_async(statement, *where_values, query_options)
        ResultPaginator.new(future) { |row| record_from_result(row, use_query_result) }
      end

      def request_meta(clause, options)
        where_clause, where_values = where_params(clause)
        select_clause, use_query_result = select_params(options)
        order_by = options[:order_by]
        order_by_clause = if order_by
                            " ORDER BY #{multi_csv_clause(order_by)}"
                          end
        limit_clause = limit_clause(options)
        request_query = "SELECT #{select_clause} FROM #{table_name}#{where_clause}#{order_by_clause}#{limit_clause}"
        [request_query, use_query_result, where_values]
      end

      def first_async(clause = {}, options = {})
        ThomasUtils::FutureWrapper.new(request_async(clause, options.merge(limit: 1))) { |results| results.first }
      end

      def request(clause, options = {})
        request_async(clause, options).get
      end

      def first(clause = {}, options = {})
        first_async(clause, options).get
      end

      def shard(hashing_column = nil, max_shard = nil, &block)
        shard_key = partition_key.last
        if hashing_column
          if block_given?
            before_save { attributes[shard_key] = (yield attributes[hashing_column].hash) }
          else
            before_save { attributes[shard_key] =  (attributes[hashing_column].hash % max_shard)}
          end
        else
          before_save { attributes[shard_key] = instance_eval(&block) }
        end
      end

      def before_save(&block)
        before_save_callbacks << block
      end

      def before_save_callbacks
        table_data.before_save_callbacks ||= []
      end

      protected

      def table_data
        @table_data ||= Attributes.new
      end

      def session
        table.connection.session
      end

      def statement(query)
        table.connection.statement(query)
      end

      def generate_table_name
        self.name.demodulize.underscore.pluralize
      end

      def define_attribute(column)
        define_method(:"#{column}=") { |value| self.attributes[column] = value }
        define_method(column.to_sym) { self.attributes[column] }
      end

      def limit_clause(options)
        limit = options[:limit]
        if limit
          integer_limit = limit.to_i
          raise "Invalid limit '#{limit}'" if integer_limit < 1
          " LIMIT #{integer_limit}"
        end
      end

      def select_params(options)
        select = options[:select]
        [select_clause(select), !!select]
      end

      def select_clause(select)
        select ? multi_csv_clause(select) : '*'
      end

      def multi_csv_clause(select)
        select.is_a?(Array) ? select.join(', ') : select
      end

      def where_params(clause)
        where_clause = where_clause(clause) if clause.size > 0
        where_values = clause.values.flatten(1)
        [where_clause, where_values]
      end

      def where_clause(clause)
        restriction = clause.map do |key, value|
          if key.is_a?(ThomasUtils::KeyComparer)
            "#{key} ?"
          else
            value.is_a?(Array) ? multi_value_restriction(key, value) : single_value_restriction(key)
          end
        end.join(' AND ')
        " WHERE #{restriction}"
      end

      def single_value_restriction(key)
        "#{key} = ?"
      end

      def multi_value_restriction(key, value)
        "#{key} IN (#{(%w(?) * value.count).join(', ')})"
      end

      def result_records(results, use_query_result)
        results.map { |row| record_from_result(row, use_query_result) }
      end

      def record_from_result(row, use_query_result)
        attributes = row_attributes(row)
        use_query_result ? QueryResult.create(attributes) : self.new(attributes)
      end

      def row_attributes(row)
        row.symbolize_keys
      end

    end
  end
end