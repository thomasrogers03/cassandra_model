require 'rspec'

module CassandraModel
  describe TableRedux do
    let(:connection_name) { nil }
    let(:table_name) { :records }

    subject { TableRedux.new(connection_name, table_name) }

    describe '#name' do
      its(:name) { is_expected.to eq(:records) }

      context 'with a different name' do
        let(:table_name) { :images }
        its(:name) { is_expected.to eq(:images) }
      end
    end

    describe '#connection' do
      it 'should be the cached cassandra connection' do
        expect(subject.connection).to eq(ConnectionCache[nil].connection)
      end

      context 'with a different connection name' do
        let(:connection_name) { :counters }
        let(:hosts) { %w(cassandra.one cassandra.two) }
        let!(:connection) { mock_connection(hosts, 'keyspace') }

        before { ConnectionCache[:counters].config = {hosts: hosts, keyspace: 'keyspace'} }

        it 'should use the specified connection' do
          expect(subject.connection).to eq(ConnectionCache[:counters].connection)
        end
      end
    end

    describe 'table column names' do
      let(:partition_key) { [:partition_key] }
      let(:clustering_columns) { [:clustering_columns] }
      let(:remaining_columns) { [:misc] }
      let(:expected_columns) { partition_key + clustering_columns + remaining_columns }

      before { mock_simple_table(table_name, partition_key, clustering_columns, remaining_columns) }

      describe '#columns' do
        its(:columns) { is_expected.to eq(expected_columns) }

        it 'should cache the columns names' do
          subject.columns
          expect(keyspace.table(table_name)).not_to receive(:columns)
          subject.columns
        end

        context 'with different columns and table name' do
          let(:table_name) { :books }
          let(:partition_key) { [:author] }
          let(:clustering_columns) { [:name, :series] }
          let(:remaining_columns) { [:summary, :rating] }

          its(:columns) { is_expected.to eq(expected_columns) }
        end
      end

    end

  end
end