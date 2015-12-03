require 'rspec'

module CassandraModel
  describe RecordDebug do
    let(:attributes) { {part: 'Partition', ck: 'Clustering', field1: 'Some data'} }
    let(:internal_attributes) do
      attributes.inject({}) do |memo, (key, value)|
        memo.merge!(:"k_#{key}" => value)
      end
    end
    let(:options) { {validate: true} }
    let(:record) { Record.new(attributes, options) }
    let(:table) { Record.send(:table) }
    let(:table_config) { Record.send(:table_config) }
    let(:table_data) { Record.send(:table_data) }
    let(:partition_key) { [:part] }
    let(:clustering_columns) { [:ck] }
    let(:fields) { [:field1] }
    let(:columns) { partition_key + clustering_columns + fields }

    before do
      mock_simple_table(:records, partition_key, clustering_columns, columns)
      allow(record).to receive(:internal_attributes).and_return(internal_attributes)
    end

    after { Record.reset! }

    describe '#debug' do
      subject { record.debug }

      its(:record) { is_expected.to eq(record) }
      its(:klass) { is_expected.to eq(Record) }
      its(:attributes) { is_expected.to eq(attributes) }
      its(:internal_attributes) { is_expected.to eq(internal_attributes) }
      its(:table) { is_expected.to eq(table) }
      its(:table_config) { is_expected.to eq(table_config) }
      its(:table_data) { is_expected.to eq(table_data) }
    end

  end
end
