require 'spec_helper'

module CassandraModel
  describe TableRedux do
    let(:connection_name) { nil }
    let(:table_name) { :records }
    let(:table) { TableRedux.new(connection_name, table_name) }
    let(:partition_key) { generate_partition_key }
    let(:clustering_columns) { generate_clustering_columns }
    let(:remaining_columns) { generate_fields }
    let(:columns) { partition_key + clustering_columns + remaining_columns }

    subject { table }

    before { mock_simple_table(table_name, partition_key, clustering_columns, remaining_columns) }

    # for time slicing
    around { |example| subject.in_context({}) { example.run } }

    describe '#in_context' do
      it 'should be the table' do
        table = subject.in_context({}) { |table| table }
        is_expected.to eq(table)
      end
    end

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

        before { ConnectionCache[:counters].config = {hosts: hosts, keyspace: 'keyspace'} }

        it 'should use the specified connection' do
          expect(subject.connection).to eq(ConnectionCache[:counters])
        end
      end
    end

    describe '#truncate!' do
      let(:allow_truncate) { true }
      let(:internal_table) { table.connection.keyspace.table(table.name) }

      before do
        internal_table.insert(generate_attributes.stringify_keys)
        subject.allow_truncation! if allow_truncate
      end

      it 'should truncate the table' do
        subject.truncate!
        expect(internal_table.rows).to be_empty
      end

      context 'with a different table name' do
        let(:table_name) { :images }

        it 'should truncate the table' do
          subject.truncate!
          expect(internal_table.rows).to be_empty
        end
      end

      context 'when truncation is not enabled' do
        let(:allow_truncate) { false }

        it 'should raise an error' do
          expect { subject.truncate! }.to raise_error("Truncation not enabled for table '#{table_name}'")
        end

        context 'with a different table name' do
          let(:table_name) { :images }

          it 'should raise an error' do
            expect { subject.truncate! }.to raise_error("Truncation not enabled for table '#{table_name}'")
          end
        end
      end
    end

    describe 'table column names' do
      shared_examples_for 'a method caching column names' do |method, table_method|
        let(:table_name) { Faker::Lorem.word }

        it 'should cache the columns names' do
          subject.public_send(method)
          expect(table.connection.keyspace.table(table_name)).not_to receive(table_method)
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

      describe '#primary_key' do
        it 'should be the combination of the partition key and the clustering columns' do
          expect(subject.primary_key).to eq(partition_key + clustering_columns)
        end

        context 'with different columns and table name' do
          let(:table_name) { :cars }
          let(:partition_key) { [:brand] }
          let(:clustering_columns) { [:colour] }

          its(:primary_key) { is_expected.to eq(partition_key + clustering_columns) }
        end
      end

    end

    describe '#reset_local_schema!' do
      let(:updated_partition_key) { generate_partition_key }
      let(:updated_clustering_columns) { generate_clustering_columns }
      let(:updated_remaining_columns) { generate_fields }
      let(:expected_columns) do
        updated_partition_key + updated_clustering_columns + updated_remaining_columns
      end
      let(:table_name) { Faker::Lorem.word }

      before do
        table.connection.keyspace.drop_table(table_name)
        mock_simple_table(
            table_name,
            updated_partition_key,
            updated_clustering_columns,
            updated_remaining_columns
        )

        table.partition_key
        table.clustering_columns
        table.columns
        table.reset_local_schema!
      end

      its(:partition_key) { is_expected.to eq(updated_partition_key) }
      its(:clustering_columns) { is_expected.to eq(updated_clustering_columns) }
      its(:columns) { is_expected.to eq(expected_columns) }
    end

    it_behaves_like 'debugging a table'
  end
end
