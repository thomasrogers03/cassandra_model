module CassandraModel
  module V2
    class ReadQuery

      def initialize(table, select_columns, restrict_columns, order, limit)
        @table_name = table.name
        @select_columns = select_columns
        @restrict_columns = restrict_columns
        @order = order
        @limit = limit
      end

      def select_clause
        "SELECT #{select_column} FROM #{@table_name}"
      end

      def restriction_clause
        if @restrict_columns.any?
          "WHERE #{restriction}"
        end
      end

      private

      def restriction
        @restrict_columns.map do |column|
          unless column.is_a?(ThomasUtils::KeyComparer)
            column = column.is_a?(Array) ? ThomasUtils::KeyComparer.new(column, 'IN') : column.to_sym.eq
          end
          if column.key.is_a?(Array)
            range_restriction(column)
          else
            single_column_restriction(column)
          end
        end * ' AND '
      end

      def single_column_restriction(column)
        "#{column} ?"
      end

      def range_restriction(column)
        "#{column} (#{%w(?) * column.key.count * ','})"
      end

      def select_column
        @select_columns.any? ? @select_columns * ',' : '*'
      end

    end
  end
end
