module CassandraModel
  shared_examples_for 'a table' do |table_suffix|

    describe '#connection' do
      its(:connection) { is_expected.to eq(klass.connection) }
    end

    describe '.reset_local_schema!' do
      let(:partition_key) { :partition }
      let(:clustering_column) { :clustering }
      let(:columns) { [:misc] }
      let(:updated_partition_key) { :updated_partition }
      let(:updated_clustering_column) { :updated_clustering }
      let(:updated_columns) { [:updated_misc] }
      let(:other_keyspace) { double(:keyspace) }

      before do
        mock_simple_table(subject.name,
                          [partition_key],
                           [clustering_column],
                           columns)
        mock_simple_table_for_keyspace(other_keyspace,
                                       subject.name,
                                       [updated_partition_key],
                                        [updated_clustering_column],
                                        updated_columns)
        allow(cluster).to receive(:keyspace).and_return(keyspace, other_keyspace)

        subject.partition_key
        subject.clustering_columns
        subject.columns
        subject.reset_local_schema!
      end

      describe 'updating the schema with the new table structure' do
        its(:partition_key) { is_expected.to eq([:updated_partition]) }
        its(:clustering_columns) { is_expected.to eq([:updated_clustering]) }
        its(:columns) { is_expected.to eq([:updated_partition, :updated_clustering, :updated_misc]) }
      end
    end

    shared_examples_for 'a set of columns' do |method, table_suffix|
      let(:column) { double(:column, name: 'partition') }
      let(:table) { double(:table, method => [column]) }
      let(:resolved_table_name) { "#{table_name}#{table_suffix}" }
      let(:other_keyspace) do
        keyspace = double(:keyspace)
        allow(keyspace).to receive(:table).with(resolved_table_name).and_return(table)
        keyspace
      end

      before do
        allow(cluster).to receive(:keyspace).and_return(other_keyspace)
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

    it_behaves_like 'a set of columns', :partition_key, table_suffix
    it_behaves_like 'a set of columns', :clustering_columns, table_suffix

    describe '.columns' do
      before do
        subject.columns = nil
      end

      it_behaves_like 'a set of columns', :columns, table_suffix
    end
  end
end