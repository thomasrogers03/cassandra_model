require 'rspec'

module CassandraModel
  describe TableRedux do
    let(:connection_name) { nil }
    let(:table_name) { :records }
    let(:table) { TableRedux.new(connection_name, table_name) }
    let(:partition_key) { [:partition_key] }
    let(:clustering_columns) { [:clustering_columns] }
    let(:remaining_columns) { [:misc] }

    subject { table }

    before { mock_simple_table(table_name, partition_key, clustering_columns, remaining_columns) }

    describe '#name' do
      its(:name) { is_expected.to eq('records') }

      context 'with a different name' do
        let(:table_name) { :images }
        its(:name) { is_expected.to eq('images') }
      end
    end

    describe '#connection' do
      it 'should be the cached cassandra connection' do
        expect(subject.connection).to eq(ConnectionCache[nil])
      end

      context 'with the connection name parameter omitted' do
        let(:table) { TableRedux.new(table_name) }

        it 'should be the cached cassandra connection' do
          expect(subject.connection).to eq(ConnectionCache[nil])
        end
      end

      context 'with a different connection name' do
        let(:connection_name) { :counters }
        let(:hosts) { %w(cassandra.one cassandra.two) }
        let!(:connection) { mock_connection(hosts, 'keyspace') }

        before { ConnectionCache[:counters].config = {hosts: hosts, keyspace: 'keyspace'} }

        it 'should use the specified connection' do
          expect(subject.connection).to eq(ConnectionCache[:counters])
        end
      end
    end

    describe 'table column names' do
      shared_examples_for 'a method caching column names' do |method, table_method|
        it 'should cache the columns names' do
          subject.public_send(method)
          expect(keyspace.table(table_name.to_s)).not_to receive(table_method)
          subject.public_send(method)
        end
      end

      describe '#columns' do
        let(:expected_columns) { partition_key + clustering_columns + remaining_columns }

        it_behaves_like 'a method caching column names', :columns, :columns

        its(:columns) { is_expected.to eq(expected_columns) }

        context 'with different columns and table name' do
          let(:table_name) { :books }
          let(:partition_key) { [:author] }
          let(:clustering_columns) { [:name, :series] }
          let(:remaining_columns) { [:summary, :rating] }

          its(:columns) { is_expected.to eq(expected_columns) }
        end
      end

      describe '#partition_key' do
        it_behaves_like 'a method caching column names', :partition_key, :partition_key

        its(:partition_key) { is_expected.to eq(partition_key) }

        context 'with different columns and table name' do
          let(:table_name) { :books }
          let(:partition_key) { [:author] }

          its(:partition_key) { is_expected.to eq(partition_key) }
        end
      end

      describe '#clustering_columns' do
        it_behaves_like 'a method caching column names', :clustering_columns, :clustering_columns

        its(:clustering_columns) { is_expected.to eq(clustering_columns) }

        context 'with different columns and table name' do
          let(:table_name) { :cars }
          let(:clustering_columns) { [:colour] }

          its(:clustering_columns) { is_expected.to eq(clustering_columns) }
        end
      end

    end

    describe '#reset_local_schema!' do
      let(:updated_partition_key) { [:updated_partition_key] }
      let(:updated_clustering_columns) { [:updated_clustering_columns] }
      let(:updated_remaining_columns) { [:misc, :extra] }
      let(:expected_columns) do
        updated_partition_key + updated_clustering_columns + updated_remaining_columns
      end
      let(:updated_keyspace) { double(:keyspace) }

      before do
        mock_simple_table_for_keyspace(updated_keyspace,
                                       table_name,
                                       updated_partition_key,
                                       updated_clustering_columns,
                                       updated_remaining_columns)
        allow(cluster).to receive(:keyspace).and_return(keyspace, updated_keyspace)

        table.partition_key
        table.clustering_columns
        table.columns
        table.reset_local_schema!
      end

      its(:partition_key) { is_expected.to eq(updated_partition_key) }
      its(:clustering_columns) { is_expected.to eq(updated_clustering_columns) }
      its(:columns) { is_expected.to eq(expected_columns) }
    end

  end
end