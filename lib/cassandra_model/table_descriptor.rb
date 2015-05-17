module CassandraModel
  class TableDescriptor < Record

    class << self
      def create_descriptor_table
        connection.execute(table_desc.to_cql)
      end

      def drop_descriptor_table
        connection.execute("DROP TABLE #{table_name}")
      end

      private

      def table_desc
        @table_desc ||= begin
          options = {name: table_name,
                     partition_key: {name: :ascii},
                     clustering_columns: {created_at: :timestamp, id: :ascii},
                     remaining_columns: {}}
          TableDefinition.new(options)
        end
      end
    end

  end
end