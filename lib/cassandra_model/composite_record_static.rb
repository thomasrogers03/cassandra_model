module CassandraModel
  module CompositeRecordStatic
    attr_writer :composite_defaults

    def columns
      @composite_columns ||= composite_columns.each { |column| define_attribute(column) }
    end

    def composite_pk_map
      @composite_pk_map ||= {}
    end

    def composite_ck_map
      @composite_ck_map ||= {}
    end

    def composite_defaults
      if @composite_defaults
        @composite_defaults.map do |row|
          row_composite_default(row)
        end
      end
    end

    private

    def composite_columns
      internal_columns.map do |column|
        trimmed_column(column, /^rk_/, composite_pk_map) ||
            trimmed_column(column, /^ck_/, composite_ck_map) ||
            column
      end.uniq
    end

    def trimmed_column(column, column_trim, map)
      column_str = column.to_s
      if column_str =~ column_trim
        column_str.gsub(column_trim, '').to_sym.tap do |result_column|
          map[result_column] = column
          map[column] = result_column
        end
      end
    end

    def where_params(clause)
      updated_clause = clause.inject({}) do |memo, (key, value)|
        memo.merge((composite_pk_map[key] || composite_ck_map[key] || key) => value)
      end

      missing_keys = partition_key - updated_clause.keys
      default_clause = composite_defaults.find { |row| row.keys == missing_keys }
      updated_clause.merge!(default_clause) if default_clause

      super(updated_clause)
    end

    def row_composite_default(row)
      row.inject({}) do |memo, (key, value)|
        memo.merge((composite_pk_map[key] || key) => value)
      end
    end

  end
end