module CassandraModel
  module DataModelling

    def self.extended(base)
      base.include(CompositeRecord)
    end

    def model_data
      inquirer = DataInquirer.new
      data_set = DataSet.new
      yield inquirer, data_set

      self.table = if table_sliced?(data_set)
        rotating_table(data_set, inquirer)
      else
        meta_table(generate_table_name, inquirer, data_set)
      end

      generate_composite_defaults_from_inquirer(inquirer)
    end

    private

    def table_sliced?(data_set)
      data_set.data_rotation[:slices]
    end

    def rotating_table(data_set, inquirer)
      table_list = data_set.data_rotation[:slices].times.map do |index|
        meta_table("#{generate_table_name}_#{index}", inquirer, data_set)
      end
      CassandraModel::RotatingTable.new(table_list, data_set.data_rotation[:frequency])
    end

    def meta_table(table_name, inquirer, data_set)
      table_definition = CassandraModel::TableDefinition.from_data_model(table_name, inquirer, data_set)
      CassandraModel::MetaTable.new(table_config.connection_name, table_definition)
    end

  end
end