module CassandraModel
  class CounterRecord < Record

    def increment_async!(options)
      counter_clause = counter_clause(options)
      row_key = internal_primary_key.values
      statement = increment_statement(counter_clause)

      future = if batch_reactor
                 execute_async_in_batch(statement, options.values + row_key)
               else
                 session.execute_async(statement, *options.values, *row_key, write_query_options)
               end
      future.on_success { execute_callback(:record_saved) }
      future.on_failure do |error|
        Logging.logger.error("Error incrementing #{self.class}: #{error}")
        execute_callback(:save_record_failed, error)
      end.then { self }
    end

    def increment!(options)
      increment_async!(options).get
    end

    def save_async
      raise NotImplementedError
    end

    private

    def internal_primary_key
      internal_attributes.slice(*self.class.internal_primary_key)
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
