require 'spec_helper'

module CassandraModel
  describe TableDescriptor do
    let(:query_results) { [] }
    let(:table_definition) do
      {name: :records,
       partition_key: {partition_key: :text},
       clustering_columns: {cluster: :text},
       remaining_columns: {meta_data: 'map<text, text>'}}
    end
    let(:definition) { TableDefinition.new(table_definition) }
    let(:time) { Time.at(0) }
    let(:attributes) do
      {name: definition.name.to_s,
       created_at: time,
       id: definition.table_id}
    end

    subject { TableDescriptor.new({}) }

    before { TableDescriptor.reset! }

    context 'when the descriptor table already exists' do
      before do
        mock_simple_table(:table_descriptors, [:name], [:created_at], [:id])
        mock_query_result([anything], [query_results])
      end

      it { is_expected.to be_a_kind_of(Record) }

      describe '.create_async' do
        let(:now) { time }

        around do |example|
          Timecop.freeze(now) { example.run }
        end

        it 'should create an entry from a table definition' do
          expect(TableDescriptor.create_async(definition).get.attributes).to eq(attributes)
        end

        context 'with a different time' do
          let(:time) { Time.at(0) }
          let(:now) { time + 4.hours }

          context 'when the time is not aligned to a day' do
            it 'should align the creation time to a day' do
              expect(TableDescriptor.create_async(definition).get.created_at).to eq(time)
            end
          end
        end

        context 'with a different table configuration' do
          let(:table_definition) do
            {name: :images,
             partition_key: {path: :text, segment: :int},
             clustering_columns: {part: :int},
             remaining_columns: {meta_data: 'map<text, text>', data: :blob}}
          end

          it 'should create an entry from a table definition' do
            expect(TableDescriptor.create_async(definition).get.attributes).to eq(attributes)
          end
        end

        context 'when the entry already exists' do
          let(:query_results) { [{'[applied]' => false}] }

          it 'should invalidate the entry' do
            expect(TableDescriptor.create_async(definition).get.valid).to eq(false)
          end
        end
      end

      describe '.create' do
        let(:record) { TableDescriptor.new(attributes) }
        let(:future_record) { MockFuture.new(record) }

        before do
          allow(TableDescriptor).to receive(:create_async).with(definition).and_return(future_record)
        end

        it 'should resolve the future returned by .create_async' do
          expect(TableDescriptor.create(definition)).to eq(record)
        end
      end

      describe '.create_descriptor_table' do
        it 'should not create the table in cassandra' do
          expect(connection).not_to receive(:execute)
          TableDescriptor.create_descriptor_table
        end
      end

      describe '.drop_descriptor_table' do
        it 'should drop the table from cassandra' do
          expected_query = 'DROP TABLE table_descriptors'
          expect(connection).to receive(:execute).with(expected_query)
          TableDescriptor.drop_descriptor_table
        end
      end
    end

    context 'when the descriptor table does not already exist' do
      describe '.create_descriptor_table' do
        it 'should create the table in cassandra' do
          expected_query = 'CREATE TABLE table_descriptors (name ascii, id ascii, created_at timestamp, PRIMARY KEY ((name), id))'
          expect(connection).to receive(:execute).with(expected_query)
          TableDescriptor.create_descriptor_table
        end
      end

      describe '.drop_descriptor_table' do
        it 'should drop the table from cassandra' do
          expect(connection).not_to receive(:execute)
          TableDescriptor.drop_descriptor_table
        end
      end
    end

  end
end
