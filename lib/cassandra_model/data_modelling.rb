module CassandraModel
  module DataModelling

    def model_data
      inquirer = DataInquirer.new
      data_set = DataSet.new
      yield inquirer, data_set
      table_definition = CassandraModel::TableDefinition.from_data_model(generate_table_name, inquirer, data_set)
      self.table = CassandraModel::MetaTable.new(table_config.connection_name, table_definition)
      generate_composite_defaults_from_inquirer(inquirer)
    end

  end
end