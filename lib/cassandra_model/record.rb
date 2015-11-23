require_relative 'query_helper'
require_relative 'meta_columns'

module CassandraModel
  class Record
    extend CassandraModel::QueryHelper
    extend CassandraModel::MetaColumns

    attr_reader :attributes, :valid, :execution_info

    Attributes = Struct.new(
        :table,

        :columns,
        :counter_columns,

        :internal_defaults,
        :composite_columns,
        :composite_pk_map,
        :composite_ck_map,

        :composite_shard_key,
    ) # Using this instead of OpenStruct, as there seems to be a bug in JRuby that causes this to get mangled over time
    ConfigureableAttributes = Struct.new(
        :table_name,
        :connection_name,

        :write_consistency,
        :read_consistency,

        :before_save_callbacks,

        :deferred_column_readers,
        :deferred_column_writers,
        :async_deferred_column_readers,
        :async_deferred_column_writers,

        :composite_defaults,

        :batch_type,
    )

    def initialize(attributes, options = {validate: true})
      ensure_attributes_accessible!
      validate_attributes!(attributes) if options[:validate]
      @execution_info = options[:execution_info]
      @valid = true
      @attributes = attributes
      self.class.after_initialize(self)
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

    def inspect
      %Q{#<#{self.class.to_s}#{inspected_validation} #{inspected_attributes}>}
    end

    alias :to_s :inspect

    def ==(rhs)
      @attributes == rhs.attributes
    end

    private

    def inspected_validation
      '(Invalidated)' unless valid
    end

    def inspected_attributes
      attributes.map do |key, value|
        %Q{#{key}: "#{value.to_s.truncate(53)}"}
      end * ', '
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
      column_values = table.primary_key.map { |column| attributes[column] }

      future = if batch_reactor
                 execute_async_in_batch(statement, column_values)
               else
                 session.execute_async(statement, *column_values, write_query_options)
               end
      future.then { self }
    end

    def internal_save_async(options = {})
      raise 'Cannot save invalidated record!' unless valid

      self.class.before_save_callbacks.map { |proc| instance_eval(&proc) }
      if !options[:skip_deferred_columns] && (self.class.deferred_column_writers || self.class.async_deferred_column_writers)
        promise = Cassandra::Future.promise
        ThomasUtils::Future.new do
          begin
            promise.fulfill(save_deferred_columns)
          rescue Exception => e
            promise.break(e)
          end
        end
        promise.future.then { save_row_async(options) }.then do |result|
          @execution_info = result.execution_info
          self
        end
      else
        save_row_async(options).then do |result|
          invalidate! if save_rejected?(result)
          @execution_info = result.execution_info
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
      column_values = table.primary_key.map { |column| attributes[column] }

      future = if batch_reactor
                 execute_async_in_batch(statement, new_attributes.values + column_values)
               else
                 session.execute_async(statement, *new_attributes.values, *column_values, write_query_options)
               end
      future.then do
        self.attributes.merge!(new_attributes)
        self
      end
    end

    def write_query_options
      {}.tap do |options|
        options[:consistency] = write_consistency if write_consistency
      end
    end

    def write_consistency
      self.class.write_consistency
    end

    def column_values
      attributes = internal_attributes
      internal_columns.map { |column| attributes[column] }
    end

    def internal_attributes
      attributes
    end

    def save_row_async(options)
      statement = statement(query_for_save(options))
      future = if batch_reactor
                 execute_async_in_batch(statement, column_values)
               else
                 session.execute_async(statement, *column_values, write_query_options)
               end
      future.on_failure do |error|
        Logging.logger.error("Error saving #{self.class}: #{error}")
      end
    end

    def execute_async_in_batch(statement, column_values)
      bound_statement = statement.bind(*column_values)
      batch_reactor.perform_within_batch(bound_statement) do |batch|
        batch.add(bound_statement)
        batch
      end
    end

    def batch_reactor
      if self.class.batch_type == :logged
        table.connection.logged_batch_reactor
      elsif self.class.batch_type == :unlogged
        table.connection.unlogged_batch_reactor
      elsif self.class.batch_type == :counter
        table.connection.counter_batch_reactor
      end
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

    alias :ensure_attributes_accessible! :columns

    def internal_columns
      self.class.internal_columns
    end

    def shard_key
      self.class.shard_key
    end

    def column_hash(hashing_column)
      Digest::MD5.hexdigest(attributes[hashing_column].to_s).unpack('L').first
    end

    class << self
      extend Forwardable

      def_delegators :table, :partition_key, :clustering_columns
      def_delegator :table, :name, :table_name
      def_delegator :table, :columns, :internal_columns
      def_delegators :table_config, :write_consistency, :read_consistency, :write_consistency=, :read_consistency=

      def table_name=(value)
        table_config.table_name = value
      end

      def connection_name=(value)
        table_config.connection_name = value
      end

      def table=(value)
        table_data.table = value
      end

      def table
        table_data.table ||= begin
          table_name = table_config.table_name || generate_table_name
          TableRedux.new(table_config.connection_name, table_name)
        end
      end

      def save_in_batch(type)
        table_config.batch_type = type
      end

      def batch_type
        table_config.batch_type
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
        where_clause = table.primary_key.map { |column| "#{column} = ?" }.join(' AND ')
        "DELETE FROM #{table_name} WHERE #{where_clause}"
      end

      def query_for_update(new_attributes)
        where_clause = table.primary_key.map { |column| "#{column} = ?" }.join(' AND ')
        set_clause = new_attributes.keys.map { |column| "#{column} = ?" }.join(', ')
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
        trace = options[:trace]
        request_query, invalidated_result, where_values = request_meta(clause, options)
        statement = statement(request_query)

        query_options = {}
        query_options[:page_size] = page_size if page_size
        query_options[:consistency] = read_consistency if read_consistency
        query_options[:trace] = trace if trace

        future = session.execute_async(statement, *where_values, query_options)
        if options[:limit] == 1
          single_result_row_future(future, invalidated_result)
        else
          paginator_result_future(future, invalidated_result)
        end
      end

      def request_meta(clause, options)
        where_clause, where_values = where_params(clause)
        select_clause, use_query_result = select_params(options)
        order_by_clause = order_by_clause(options[:order_by])
        limit_clause = limit_clause(options)
        request_query = "SELECT #{select_clause} FROM #{table_name}#{where_clause}#{order_by_clause}#{limit_clause}"
        [request_query, use_query_result, where_values]
      end

      def order_by_clause(order_by)
        " ORDER BY #{multi_csv_clause(order_by)}" if order_by
      end

      def first_async(clause = {}, options = {})
        request_async(clause, options.merge(limit: 1))
      end

      def request(clause, options = {})
        request_async(clause, options).get
      end

      def first(clause = {}, options = {})
        first_async(clause, options).get
      end

      def shard(hashing_column = nil, max_shard = nil, &block)
        if hashing_column
          if block_given?
            hashing_shard(hashing_column, &block)
          else
            modulo_shard(hashing_column, max_shard)
          end
        else
          manual_shard(&block)
        end
      end

      def before_save(&block)
        before_save_callbacks << block
      end

      def before_save_callbacks
        table_config.before_save_callbacks ||= []
      end

      def shard_key
        partition_key.last
      end

      protected

      def table_data
        @table_data ||= Attributes.new
      end

      def table_config
        @table_config ||= ConfigureableAttributes.new
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

      def paginator_result_future(future, invalidated_result)
        ResultPaginator.new(future) { |row, execution_info| record_from_result(row, execution_info, invalidated_result) }
      end

      def single_result_row_future(future, invalidated_result)
        future.then do |rows|
          record_from_result(rows.first, rows.execution_info, invalidated_result) if rows.first
        end
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

      def record_from_result(row, execution_info, invalidate_result)
        attributes = row_attributes(row)
        new(attributes, execution_info: execution_info).tap { |result| result.invalidate! if invalidate_result }
      end

      def row_attributes(row)
        row.symbolize_keys
      end

      def manual_shard(&block)
        before_save { attributes[shard_key] = instance_eval(&block) }
      end

      def modulo_shard(hashing_column, max_shard)
        before_save { attributes[shard_key] = (column_hash(hashing_column) % max_shard) }
      end

      def hashing_shard(hashing_column)
        before_save { attributes[shard_key] = (yield column_hash(hashing_column)) }
      end

    end
  end
end
