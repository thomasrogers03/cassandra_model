module CassandraModel
  class MetaTable < Record

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
          columns = {name: :ascii, created_at: :timestamp, id: :ascii}
          options = {name: table_name, columns: columns, partition_key: [:name], clustering_columns: [:created_at, :id]}
          TableDefinition.new(options)
        end
      end
    end

  end
end