module CassandraModel
  class TableDescriptor < Record

    class << self
      def create_async(table_definition)
        super(table_descriptor(table_definition), check_exists: true)
      end

      def create(table_definition)
        create_async(table_definition).get
      end

      def create_descriptor_table
        session.execute(table_desc.to_cql(no_id: true)) unless descriptor_table_exists?
      end

      def drop_descriptor_table
        session.execute("DROP TABLE #{table_name}")
      end

      private

      def descriptor_table_exists?
        table.connection.keyspace.table(table.name)
      end

      def table_descriptor(table_definition)
        {name: table_definition.name.to_s,
         created_at: rounded_time,
         id: table_definition.table_id}
      end

      def rounded_time
        Time.at((Time.now.to_i / 1.day) * 1.day)
      end

      def table_desc
        @table_desc ||= begin
          options = {name: table_name,
                     partition_key: {name: :ascii},
                     clustering_columns: {id: :ascii},
                     remaining_columns: {created_at: :timestamp}}
          TableDefinition.new(options)
        end
      end
    end

  end
end