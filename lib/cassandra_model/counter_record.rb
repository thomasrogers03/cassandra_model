module CassandraModel
  class CounterRecord < Record

    def increment_async!(options)
      row_key = partition_key + clustering_columns
      row_key_attributes = row_key_attributes(row_key)
      where_clause = where_clause(row_key)
      counter_clause = counter_clause(options)
      statement = increment_statement(counter_clause, where_clause)
      session.execute_async(statement, *options.values, *row_key_attributes, {}).on_failure do |error|
        Logging.logger.error("Error incrementing #{self.class}: #{error}")
      end.then { self }
    end

    def increment!(options)
      increment_async!(options).get
    end

    def save_async
      raise NotImplementedError
    end

    private

    def row_key_attributes(row_key)
      row_key.map { |key| internal_attributes[key] }
    end

    def increment_statement(counter_clause, where_clause)
      query = "UPDATE #{self.class.table_name} SET #{counter_clause} WHERE #{where_clause}"
      statement(query)
    end

    def counter_clause(options)
      options.keys.map { |column| "#{column} = #{column} + ?" }.join(', ')
    end

    def where_clause(row_key)
      row_key.map { |key| "#{key} = ?" }.join(' AND ')
    end

    def clustering_columns
      self.class.clustering_columns
    end

    def partition_key
      self.class.partition_key
    end

    class << self
      def counter_columns
        table_data.counter_columns ||= columns - (partition_key + clustering_columns)
      end

      def request_async(clause, options = {})
        selected_columns = if options[:select]
                             options[:select] | counter_columns
                           else
                             counter_columns
                           end
        super(clause, options.merge(select: selected_columns))
      end
    end
  end
end