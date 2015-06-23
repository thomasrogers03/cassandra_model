module CassandraModel
  module DataModelling

    def self.extended(base)
      base.include(CompositeRecord)
    end

    def model_data
      inquirer = DataInquirer.new
      data_set = DataSet.new
      yield inquirer, data_set

      self.table = if data_set.data_rotation[:slices]
        rotating_table(data_set, inquirer)
      else
        meta_table(generate_table_name, inquirer, data_set)
      end

      generate_composite_defaults_from_inquirer(inquirer)
    end

    private

    def rotating_table(data_set, inquirer)
      table_list = 2.times.map do |index|
        meta_table("#{generate_table_name}_#{index}", inquirer, data_set)
      end
      CassandraModel::RotatingTable.new(table_list, 1.day)
    end

    def meta_table(table_name, inquirer, data_set)
      table_definition = CassandraModel::TableDefinition.from_data_model(table_name, inquirer, data_set)
      CassandraModel::MetaTable.new(table_config.connection_name, table_definition)
    end

  end
end