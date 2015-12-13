module CassandraModel
  module CompositeRecordStatic
    PK_MUTEX = Mutex.new
    CK_MUTEX = Mutex.new

    extend Forwardable

    def_delegator :table_config, :composite_defaults=

    def partition_key
      table_data.composite_partition_key ||= internal_partition_key.map { |column| trimmed_column(column, /^rk_/, composite_pk_map) || column }
    end

    def clustering_columns
      table_data.composite_clustering_columns ||= internal_clustering_columns.map { |column| trimmed_column(column, /^ck_/, composite_ck_map) || column }
    end

    def primary_key
      table_data.composite_primary_key ||= (internal_partition_key + internal_clustering_columns).map do |column|
        trimmed_column(column, /^rk_/, composite_pk_map) ||
            trimmed_column(column, /^ck_/, composite_ck_map) ||
            column
      end.uniq
    end

    def columns
      table_data.composite_columns ||= composite_columns.each { |column| define_attribute(column) }
    end

    def composite_pk_map
      unless table_data.composite_pk_map
        PK_MUTEX.synchronize do
          return table_data.composite_pk_map if table_data.composite_pk_map

          table_data.composite_pk_map = {}
          columns
        end
      end
      table_data.composite_pk_map
    end

    def composite_ck_map
      unless table_data.composite_ck_map
        CK_MUTEX.synchronize do
          return table_data.composite_ck_map if table_data.composite_ck_map

          table_data.composite_ck_map = {}
          columns
        end
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

    def shard_key
      table_data.composite_shard_key ||= begin
        column = super
        column =~ /^rk_/ ? composite_pk_map[column] : column
      end
    end

    def restriction_attributes(restriction)
      updated_restriction = restriction.inject({}) do |memo, (key, value)|
        updated_key = key_for_where_params(key)
        memo.merge!(updated_key => value)
      end

      missing_keys = Set.new(internal_partition_key - updated_restriction.keys)
      default_clause = composite_defaults.find { |row| (missing_keys ^ row.keys).empty? }
      updated_restriction.merge!(default_clause) if default_clause
      updated_restriction
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
      select = select_columns(select) if select
      super(select)
    end

    def order_by_clause(order_by)
      order_by = select_columns(order_by) if order_by
      super(order_by)
    end

    def select_columns(select)
      select.map do |column|
        if internal_columns.include?(column)
          column
        else
          mapped_column(column)
        end
      end
    end

    def where_params(clause)
      super restriction_attributes(clause)
    end

    def key_for_where_params(key)
      key.is_a?(ThomasUtils::KeyComparer) ? mapped_key_comparer(key) : mapped_key(key)
    end

    def mapped_key_comparer(key)
      mapped_key = key.key.is_a?(Array) ? key.key.map { |part| mapped_ck(part) } : mapped_ck(key.key)
      key.new_key(mapped_key)
    end

    def mapped_key(key)
      composite_pk_map[key] || mapped_ck(key)
    end

    def mapped_ck(key)
      composite_ck_map[key] || key
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
