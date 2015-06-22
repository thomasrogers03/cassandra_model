module CassandraModel
  module CompositeRecordStatic
    extend Forwardable

    def_delegator :table_config, :composite_defaults=

    def columns
      table_data.composite_columns ||= composite_columns.each { |column| define_attribute(column) }
    end

    def composite_pk_map
      unless table_data.composite_pk_map
        table_data.composite_pk_map = {}
        columns
      end
      table_data.composite_pk_map
    end

    def composite_ck_map
      unless table_data.composite_ck_map
        table_data.composite_ck_map = {}
        columns
      end
      table_data.composite_ck_map
    end

    def composite_defaults
      table_data.internal_defaults ||= build_composite_map
    end

    def generate_composite_defaults(column_defaults, truth_table)
      table_config.composite_defaults = truth_table.map { |row| column_defaults.except(*row) }
    end

    def generate_composite_defaults_from_inquirer(inquirer)
      table_config.composite_defaults = inquirer.composite_rows.map do |row|
        row.inject({}) do |memo, column|
          memo.merge!(column => inquirer.column_defaults[column])
        end
      end
    end

    private

    def build_composite_map
      if table_config.composite_defaults
        table_config.composite_defaults.map { |row| row_composite_default(row) }
      end
    end

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

    def select_clause(select)
      select = mapped_select_columns(select) if select
      super(select)
    end

    def mapped_select_columns(select)
      select.map do |column|
        if internal_columns.include?(column)
          column
        else
          mapped_column(column)
        end
      end
    end

    def where_params(clause)
      updated_clause = clause.inject({}) do |memo, (key, value)|
        memo.merge!((composite_pk_map[key] || composite_ck_map[key] || key) => value)
      end

      missing_keys = Set.new(partition_key - updated_clause.keys)
      default_clause = composite_defaults.find { |row| (missing_keys ^ row.keys).empty? }
      updated_clause.merge!(default_clause) if default_clause

      super(updated_clause)
    end

    def row_composite_default(row)
      row.inject({}) do |memo, (key, value)|
        memo.merge!(composite_default_row_key(key) => value)
      end
    end

    def composite_default_row_key(key)
      composite_pk_map[key] || key
    end

    def row_attributes(row)
      row = super(row)

      row.inject({}) do |memo, (column, value)|
        if column =~ /^rk_/ || column =~ /^ck_/
          memo.merge!(mapped_column(column) => value)
        else
          memo.merge!(column => value)
        end
      end
    end

    def mapped_column(column)
      (composite_ck_map[column] || composite_pk_map[column] || column)
    end

  end
end