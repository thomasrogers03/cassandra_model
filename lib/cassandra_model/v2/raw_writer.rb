module CassandraModel
  module V2
    class RawWriter

      def initialize(session, table)
        @session = session
        @table = table
        @statement = session.prepare(query)
      end

      def write(column_values)
        bound_statement = @statement.bind(*column_values)
        future = @session.execute_async(bound_statement)
        Observable.create_observation(future)
      end

      private

      attr_reader :table

      def query
        column_names = table.columns.map(&:name)
        "INSERT INTO #{table.name} (#{column_names * ','}) VALUES (#{%w(?) * column_names.count * ','})"
      end

    end
  end
end
