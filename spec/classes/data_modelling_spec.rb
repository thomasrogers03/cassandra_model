require 'spec_helper'

module CassandraModel
  describe DataModelling do
    class MockDataModel
      extend DataModelling

      class << self
        attr_reader :table_definition, :table, :composite_defaults
        attr_accessor :connection_name

        def table=(table)
          # hack to test the table schema
          @table_definition = table.instance_variable_get(:@table_definition)
          @table = table
        end

        def table_config
          self
        end

        def generate_table_name
          # nothing here...
        end

        def generate_composite_defaults_from_inquirer(inquirer)
          @composite_defaults = inquirer.composite_rows.map do |row|
            row.inject({}) { |memo, column| memo.merge!(column => inquirer.column_defaults[column]) }
          end
        end
      end
    end

    let(:connection_name) { nil }
    let(:table_name) { :cars }

    before do
      MockDataModel.connection_name = connection_name
      allow(MockDataModel).to receive(:generate_table_name).and_return(table_name)
    end

    describe '#model_data' do
      let(:table_attributes) do
        {
            name: table_name,
            partition_key: {rk_make: :text, rk_model: :text},
            clustering_columns: {ck_make: :text, ck_model: :text},
            remaining_columns: {description: :text}
        }
      end

      context 'with a basic inquiry/data set pair' do
        before do
          MockDataModel.model_data do |inquirer, data_set|
            inquirer.knows_about(:make, :model)
            inquirer.knows_about(:make)
            inquirer.knows_about(:model)
            data_set.is_defined_by(:make, :model)
            data_set.knows_about(:description)
          end
        end

        it 'should create a meta table' do
          expect(MockDataModel.table).to be_a_kind_of(MetaTable)
        end

        it 'should use the specified connection name' do
          expect(MockDataModel.table.connection).to eq(CassandraModel::ConnectionCache[connection_name])
        end

        it 'should create a table based on an inquirer/data set pair' do
          expect(MockDataModel.table_definition).to eq(CassandraModel::TableDefinition.new(table_attributes))
        end

        it 'should generate composite defaults from the inquirer' do
          expect(MockDataModel.composite_defaults).to eq([{model: ''}, {make: ''}])
        end
      end

      context 'with a different table setup' do
        let(:connection_name) { :single }
        let(:table_name) { :images }
        let(:table_attributes) do
          {
              name: table_name,
              partition_key: {rk_artist: :text, rk_year: :int},
              clustering_columns: {ck_price: :double, ck_artist: :text, ck_year: :int},
              remaining_columns: {damages: 'map<text,text>'}
          }
        end

        before do
          MockDataModel.model_data do |inquirer, data_set|
            inquirer.knows_about(:artist, :year)
            inquirer.knows_about(:year)
            inquirer.knows_about(:artist)
            inquirer.defaults(:year).to(1990)
            inquirer.defaults(:artist).to('NULL')

            data_set.is_defined_by(:price, :artist, :year)
            data_set.retype(:price).to(:double)
            data_set.retype(:year).to(:int)
            data_set.knows_about(:damages)
            data_set.retype(:damages).to('map<text,text>')
          end
        end

        it 'should use the specified connection name' do
          expect(MockDataModel.table.connection).to eq(CassandraModel::ConnectionCache[connection_name])
        end

        it 'should create a table based on an inquirer/data set pair' do
          expect(MockDataModel.table_definition).to eq(CassandraModel::TableDefinition.new(table_attributes))
        end

        it 'should generate composite defaults from the inquirer' do
          expect(MockDataModel.composite_defaults).to eq([{artist: 'NULL'}, {year: 1990}])
        end
      end
    end

  end
end