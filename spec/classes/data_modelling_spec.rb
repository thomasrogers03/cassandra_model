require 'spec_helper'

module CassandraModel
  describe DataModelling do

    let(:data_model_class) do
      Class.new do
        extend DataModelling

        class << self
          attr_reader :table_definition, :composite_defaults, :internal_columns
          attr_accessor :connection_name, :generated_table_name, :table_name, :table

          def table_config
            self
          end

          alias :generate_table_name :generated_table_name

          def columns
            @internal_columns = @composite_defaults.map(&:keys).flatten.uniq
          end

          def deferred_column(name, options)
            deferred_columns[name] = options
          end

          def generate_composite_defaults_from_inquirer(inquirer)
            @composite_defaults = inquirer.composite_rows.map do |row|
              row.inject({}) { |memo, column| memo.merge!(column => inquirer.column_defaults[column]) }
            end
          end

          def deferred_columns
            @deferred_columns ||= {}
          end
        end
      end
    end

    let(:connection_name) { nil }
    let(:generated_table_name) { :cars }
    let(:table_name) { nil }
    let(:table_suffix) { 'ac6c09f271f7464bb2ac77992153eaef' }
    let(:full_table_name) { "#{table_name}_#{table_suffix}" if table_name }
    let(:invalid_table_descriptor) { TableDescriptor.new({}).tap { |desc| desc.invalidate! } }

    subject { data_model_class.new }

    it { is_expected.to be_a_kind_of(CompositeRecord) }

    before do
      data_model_class.table_name = table_name
      mock_simple_table(full_table_name, [:make, :model], [:make, :model], [:description]) if full_table_name
      mock_simple_table(:table_descriptors, [:name], [:created_at], [:id])
      allow(TableDescriptor).to receive(:create).and_return(invalid_table_descriptor)
      allow_any_instance_of(MetaTable).to receive(:sleep)
      data_model_class.connection_name = connection_name
      data_model_class.generated_table_name = generated_table_name
    end

    describe '#table_proprties=' do
      let(:properties) { nil }
      let(:table_attributes) do
        {
            name: generated_table_name,
            partition_key: {rk_make: :text},
            clustering_columns: {ck_model: :text},
            remaining_columns: {},
            properties: properties || {}
        }
      end
      let(:table_definition) { TableDefinition.new(table_attributes) }

      before do
        data_model_class.table_properties = properties
        data_model_class.model_data do |inquirer, data_set|
          inquirer.knows_about(:make)
          data_set.is_defined_by(:model)
        end
      end

      it 'should create the meta table using the default properties' do
        expect(data_model_class.table).to eq(MetaTable.new(connection_name, table_definition))
      end

      context 'with some properties' do
        let(:properties) { {clustering_order: {value: :desc}, compaction: {class: 'LeveledCompactionStrategy'}} }

        it 'should create the meta table using the default properties' do
          expect(data_model_class.table).to eq(MetaTable.new(connection_name, table_definition))
        end
      end
    end

    describe '#model_data' do
      let(:table_attributes) do
        {
            name: generated_table_name,
            partition_key: {rk_make: :text, rk_model: :text},
            clustering_columns: {ck_make: :text, ck_model: :text},
            remaining_columns: {description: :text}
        }
      end
      let(:table_definition) { TableDefinition.new(table_attributes) }

      context 'with a basic inquiry/data set pair' do
        before do
          data_model_class.model_data do |inquirer, data_set|
            inquirer.knows_about(:make, :model)
            inquirer.knows_about(:make)
            inquirer.knows_about(:model)
            data_set.is_defined_by(:make, :model)
            data_set.knows_about(:description)
          end
        end

        it 'should create a meta table' do
          expect(data_model_class.table).to be_a_kind_of(MetaTable)
        end

        it 'should create a table based on an inquirer/data set pair' do
          expect(data_model_class.table).to eq(MetaTable.new(connection_name, table_definition))
        end

        it 'should generate composite defaults from the inquirer' do
          expect(data_model_class.composite_defaults).to eq([{model: ''}, {make: ''}])
        end

        it 'should ensure the table is persisted' do
          expect(data_model_class.internal_columns).to match_array([:make, :model])
        end

        context 'when overriding the table name' do
          let(:table_name) { :super_cars }

          it { expect(data_model_class.table.name).to start_with(table_name.to_s) }
        end
      end

      context 'when a table rotation has been specified' do
        let(:table_slices) { 2 }
        let(:rotation_interval) { 1.day }
        let(:table_attributes) do
          {
              name: generated_table_name,
              partition_key: {rk_make: :text},
              clustering_columns: {ck_model: :text},
              remaining_columns: {}
          }
        end
        let!(:rotating_tables) do
          table_slices.times.map do |index|
            updated_attributes = table_attributes.merge(name: "#{generated_table_name}_#{index}")
            table_definition = TableDefinition.new(updated_attributes)
            mock_simple_table(table_definition.name_in_cassandra, [:rk_make], [:ck_model], [])
            MetaTable.new(connection_name, table_definition)
          end
        end
        let(:table_suffix) { '3f51ba68dd4a3295d013082186dd5d76' }

        before do
          table_slices.times { |index| mock_simple_table("#{table_name}_#{index}_#{table_suffix}", [:make], [:model], []) }
          data_model_class.model_data do |inquirer, data_set|
            inquirer.knows_about(:make)
            data_set.is_defined_by(:model)
            data_set.rotates_storage_across(table_slices).tables_every(rotation_interval)
          end
        end

        it 'should create sufficient tables for rotation using the specified interval' do
          expect(data_model_class.table).to eq(RotatingTable.new(rotating_tables, rotation_interval))
        end

        context 'with a different table name' do
          let(:generated_table_name) { :planes }

          it 'should use the proper table' do
            expect(data_model_class.table).to eq(RotatingTable.new(rotating_tables, rotation_interval))
          end
        end

        context 'with a different rotating setup' do
          let(:table_slices) { 3 }
          let(:rotation_interval) { 1.week }

          it 'should create sufficient tables for rotation using the specified interval' do
            expect(data_model_class.table).to eq(RotatingTable.new(rotating_tables, rotation_interval))
          end
        end

        context 'when overriding the table name' do
          let(:table_name) { :super_cars }

          it { expect(data_model_class.table.name).to match(/^#{table_name}_\d/) }
        end
      end

      context 'with a different table setup' do
        let(:connection_name) { :single }
        let(:generated_table_name) { :images }
        let(:table_attributes) do
          {
              name: generated_table_name,
              partition_key: {rk_artist: :text, rk_year: :int},
              clustering_columns: {ck_price: :double, ck_artist: :text, ck_year: :int},
              remaining_columns: {damages: 'map<text,text>'}
          }
        end

        before do
          data_model_class.model_data do |inquirer, data_set|
            inquirer.knows_about(:artist, :year)
            inquirer.knows_about(:year)
            inquirer.knows_about(:artist)
            inquirer.defaults(:year).to(1990)
            inquirer.defaults(:artist).to('NULL')

            data_set.is_defined_by(:price, :artist, :year)
            data_set.change_type_of(:price).to(:double)
            data_set.change_type_of(:year).to(:int)
            data_set.knows_about(:damages)
            data_set.change_type_of(:damages).to('map<text,text>')
          end
        end

        it 'should create a table based on an inquirer/data set pair' do
          expect(data_model_class.table).to eq(MetaTable.new(connection_name, table_definition))
        end

        it 'should generate composite defaults from the inquirer' do
          expect(data_model_class.composite_defaults).to eq([{artist: 'NULL'}, {year: 1990}])
        end
      end
    end

    describe '#serialized_column' do
      let(:column) { Faker::Lorem.word.to_sym }
      let(:serialized_column) { :"#{column}_data" }
      let(:serializer) { Marshal }
      let(:value) { Faker::Lorem.paragraphs }
      let(:serialized_value) { serializer.dump(value) }

      before { data_model_class.serialized_column(column, serializer) }

      it 'should define a deferred column method for loading the serialized column' do
        result = data_model_class.deferred_columns[column][:on_load].call(serialized_column => serialized_value)
        expect(result).to eq(value)
      end

      it 'should define a deferred column method for saving the serialized column' do
        attributes = {}
        data_model_class.deferred_columns[column][:on_save].call(attributes, value)
        expect(attributes).to include(serialized_column => serialized_value)
      end

      context 'with a different serializer' do
        let(:serializer) { JSON }

        it 'should define a deferred column method for loading the serialized column' do
          result = data_model_class.deferred_columns[column][:on_load].call(serialized_column => serialized_value)
          expect(result).to eq(value)
        end

        it 'should define a deferred column method for saving the serialized column' do
          attributes = {}
          data_model_class.deferred_columns[column][:on_save].call(attributes, value)
          expect(attributes).to include(serialized_column => serialized_value)
        end
      end

      context 'when the value is nil' do
        let(:value) { nil }
        let(:serialized_value) { nil }

        it 'should load as nil' do
          result = data_model_class.deferred_columns[column][:on_load].call(serialized_column => serialized_value)
          expect(result).to be_nil
        end

        it 'should save as nil' do
          attributes = {}
          data_model_class.deferred_columns[column][:on_save].call(attributes, value)
          expect(attributes).to include(serialized_column => nil)
        end
      end

    end

  end
end
