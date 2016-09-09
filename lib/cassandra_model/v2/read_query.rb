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
          restriction = @restrict_columns.map do |column|
            column = column.to_sym.eq unless column.is_a?(ThomasUtils::KeyComparer)
            if column.key.is_a?(Array)
              "(#{column.key * ', '}) <= (#{%w(?) * column.key.count * ', '})"
            else
              "#{column} ?"
            end
          end * ' AND '
          "WHERE #{restriction}"
        end
      end

      private

      def select_column
        @select_columns.any? ? @select_columns * ', ' : '*'
      end

    end
  end
end
