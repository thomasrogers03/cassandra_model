require 'rspec'

module CassandraModel
  describe Table do
    let(:table_name) { :records }
    let(:cluster) { double(:cluster, connect: connection) }
    let(:connection) { double(:connection) }
    let(:column_object) { double(:column, name: 'partition') }
    let(:table_object) { double(:table, columns: [column_object]) }
    let(:keyspace) { double(:keyspace, table: table_object) }

    subject { Table.new(table_name) }

    before do
      Table.reset!
      allow(Cassandra).to receive(:cluster).and_return(cluster)
      allow(cluster).to receive(:keyspace).with(Record.config[:keyspace]).and_return(keyspace)
    end

    it_behaves_like 'a model with a connection', Table

    describe '#connection' do
      its(:connection) { is_expected.to eq(Table.connection) }
    end

    describe '#name' do
      its(:name) { is_expected.to eq('records') }
    end

    describe '.reset_local_schema!' do
      let(:partition_key) { double(:column, name: 'partition') }
      let(:clustering_column) { double(:column, name: 'clustering') }
      let(:columns) { [partition_key, clustering_column] }
      let(:updated_partition_key) { double(:column, name: 'updated_partition') }
      let(:updated_clustering_column) { double(:column, name: 'updated_clustering') }
      let(:updated_columns) { [updated_partition_key, updated_clustering_column] }
      let(:table_object) do
        table = double(:table)
        allow(table).to receive(:partition_key).and_return([partition_key], [updated_partition_key])
        allow(table).to receive(:clustering_columns).and_return([clustering_column], [updated_clustering_column])
        allow(table).to receive(:columns).and_return(columns, updated_columns)
        table
      end

      before do
        subject.partition_key
        subject.clustering_columns
        subject.columns
        subject.reset_local_schema!
      end

      describe 'updating the schema with the new table structure' do
        its(:partition_key) { is_expected.to eq([:updated_partition]) }
        its(:clustering_columns) { is_expected.to eq([:updated_clustering]) }
        its(:columns) { is_expected.to eq([:updated_partition, :updated_clustering]) }
      end
    end

    shared_examples_for 'a set of columns' do |method|
      let(:column) { double(:column, name: 'partition') }
      let(:table) { double(:table, method => [column]) }
      let(:table_name) { 'records' }
      let(:keyspace) do
        keyspace = double(:keyspace)
        allow(keyspace).to receive(:table).with(table_name).and_return(table)
        keyspace
      end

      it 'should be the partition key for this table' do
        expect(subject.send(method)).to eq([:partition])
      end

      it 'should cache the partition key' do
        subject.send(method)
        expect(keyspace).not_to receive(:table)
        subject.send(method)
      end

      context 'with a different table name' do
        let(:table_name) { 'image_data' }

        it 'should be the partition key for that table' do
          expect(subject.send(method)).to eq([:partition])
        end
      end

      context 'with multiple partition key parts' do
        let(:other_column) { double(:column, name: 'partition_part_two') }
        let(:table) { double(:table, method => [column, other_column]) }

        it 'should be the partition key for this table' do
          expect(subject.send(method)).to eq([:partition, :partition_part_two])
        end
      end
    end

    it_behaves_like 'a set of columns', :partition_key
    it_behaves_like 'a set of columns', :clustering_columns

    describe '.columns' do
      before do
        subject.columns = nil
      end

      it_behaves_like 'a set of columns', :columns
    end

  end
end