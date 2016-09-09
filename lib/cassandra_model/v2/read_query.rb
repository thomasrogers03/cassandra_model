module CassandraModel
  module V2
    class ReadQuery
      attr_reader :column_names, :hash

      def initialize(table, select_columns, restrict_columns, order, limit)
        @table_name = table.name
        @select_columns = select_columns
        @column_names = @select_columns.any? ? @select_columns : table.columns.map(&:name)
        @restrict_columns = restrict_columns
        @order = order
        @limit = limit
        @hash = [@table_name, @select_columns, @restrict_columns, @order, @limit].map(&:hash).reduce(&:+)
      end

      def select_clause
        "SELECT #{select_columns_clause} FROM #{@table_name}"
      end

      def restriction_clause
        " WHERE #{restriction}" if @restrict_columns.any?
      end

      def ordering_clause
        " ORDER BY #{@order * ','}" if @order.any?
      end

      def limit_clause
        ' LIMIT ?' if @limit
      end

      def eql?(rhs)
        rhs.table_name == table_name &&
            rhs.select_columns == select_columns &&
            rhs.restrict_columns == restrict_columns &&
            rhs.order == order &&
            rhs.limit == limit
      end

      def to_s
        "#{select_clause}#{restriction_clause}#{ordering_clause}#{limit_clause}"
      end

      protected

      attr_reader :table_name, :select_columns, :restrict_columns, :order, :limit

      private

      def restriction
        @restrict_columns.map do |column|
          column = key_comparer(column) unless column.is_a?(ThomasUtils::KeyComparer)
          column_restriction(column)
        end * ' AND '
      end

      def column_restriction(column)
        column.key.is_a?(Array) ? range_restriction(column) : single_column_restriction(column)
      end

      def key_comparer(column)
        column.is_a?(Array) ? ThomasUtils::KeyComparer.new(column, 'IN') : column.to_sym.eq
      end

      def single_column_restriction(column)
        "#{column} ?"
      end

      def range_restriction(column)
        "#{column} (#{%w(?) * column.key.count * ','})"
      end

      def select_columns_clause
        @select_columns.any? ? @select_columns * ',' : '*'
      end

    end
  end
end
