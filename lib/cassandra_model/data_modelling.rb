module CassandraModel
  module DataModelling

    def self.extended(base)
      base.send(:include, CompositeRecord)
    end

    def model_data
      inquirer = DataInquirer.new
      data_set = DataSet.new
      yield inquirer, data_set

      self.table = if table_sliced?(data_set)
                     rotating_table(data_set, inquirer)
                   else
                     meta_table(table_prefix, inquirer, data_set)
                   end

      generate_composite_defaults_from_inquirer(inquirer)
      columns
    end

    def serialized_column(column, serializer)
      serialized_column = :"#{column}_data"
      deferred_column column, on_load: ->(attributes) { serializer.load(attributes[serialized_column]) },
                      on_save: ->(attributes, value) { attributes[serialized_column] = serializer.dump(value) }
    end

    private

    def table_sliced?(data_set)
      data_set.data_rotation[:slices]
    end

    def rotating_table(data_set, inquirer)
      table_list = data_set.data_rotation[:slices].times.map do |index|
        meta_table("#{table_prefix}_#{index}", inquirer, data_set)
      end
      CassandraModel::RotatingTable.new(table_list, data_set.data_rotation[:frequency])
    end

    def table_prefix
      table_config.table_name || generate_table_name
    end

    def meta_table(table_name, inquirer, data_set)
      table_definition = CassandraModel::TableDefinition.from_data_model(table_name, inquirer, data_set)
      CassandraModel::MetaTable.new(table_config.connection_name, table_definition)
    end

  end
end
