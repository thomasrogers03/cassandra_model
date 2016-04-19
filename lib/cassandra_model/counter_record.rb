module CassandraModel
  class CounterRecord < Record

    def increment_async!(options)
      internal_increment_async!(options)
    end

    def increment!(options)
      increment_async!(options).get
    end

    protected

    def internal_save_async(options = {})
      raise NotImplementedError
    end

    def internal_increment_async!(options)
      counter_clause = counter_clause(options)
      row_key = internal_primary_key.values
      statement = increment_statement(counter_clause)
      column_values = options.values + row_key

      validation_error = validate_primary_key!(statement, column_values)
      return validation_error if validation_error

      future = if batch_reactor
                 execute_async_in_batch(statement, column_values)
               else
                 session.execute_async(statement, *column_values, write_query_options)
               end
      future.on_success { execute_callback(:record_saved) }
      future.on_failure do |error|
        Logging.logger.error("Error incrementing #{self.class}: #{error}")
        execute_callback(:save_record_failed, error, statement, column_values)
      end.then { self }
    end

    private

    def internal_primary_key
      self.class.internal_primary_key.inject({}) { |memo, key| memo.merge!(key => internal_attributes[key]) }
    end

    def increment_statement(counter_clause)
      query = "UPDATE #{self.class.table_name} SET #{counter_clause} WHERE #{update_restriction}"
      statement(query)
    end

    def counter_clause(options)
      options.keys.map { |column| "#{column} = #{column} + ?" }.join(', ')
    end

    def update_restriction
      self.class.internal_primary_key.map { |key| "#{key} = ?" }.join(' AND ')
    end

    class << self
      def counter_columns
        table_data.counter_columns ||= columns - (partition_key + clustering_columns)
      end

      def save_in_batch
        table_config.batch_type = :counter
      end
    end
  end
end
