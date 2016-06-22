require 'spec_helper'

module CassandraModel
  describe CounterRecord do
    class ImageCounter < CounterRecord
    end

    let(:partition_key_types) { generate_partition_key_with_types }
    let(:partition_key) { partition_key_types.keys }
    let(:clustering_columns_types) { generate_clustering_columns_with_types }
    let(:clustering_columns) { clustering_columns_types.keys }
    let(:counter_columns) { [:counter] }
    let(:counter_columns_types) { generate_counter_fields(counter_columns) }
    let(:columns) { partition_key + clustering_columns + counter_columns }
    let(:attributes) { generate_primary_key }

    subject { CounterRecord.new({}) }

    before do
      mock_table(:counter_records, partition_key_types, clustering_columns_types, counter_columns_types)
      mock_table(:image_counters, partition_key_types, clustering_columns_types, counter_columns_types)
      allow(Logging.logger).to receive(:error)
    end

    after do
      CounterRecord.reset!
      ImageCounter.reset!
    end

    it { is_expected.to be_a_kind_of(Record) }

    describe '.counter_columns' do
      it 'should be the columns not part of the partition or clustering keys' do
        expect(CounterRecord.counter_columns).to eq(counter_columns)
      end

      it 'should cache the counter columns' do
        CounterRecord.counter_columns
        expect(CounterRecord).not_to receive(:table)
        CounterRecord.counter_columns
      end

      context 'with a different counter columns' do
        let(:counter_columns) { [:different_counter, :extra_counter] }

        it 'should be the columns not part of the partition or clustering keys' do
          expect(CounterRecord.counter_columns).to eq(counter_columns)
        end
      end
    end

    describe '#increment_async!' do
      it 'should return a ThomasUtils::Observation' do
        expect(CounterRecord.new(attributes).increment_async!(counter: 1)).to be_a_kind_of(ThomasUtils::Observation)
      end

      it 'should increment the specified counter by the specified amount' do
        CounterRecord.new(attributes).increment_async!(counter: 1).get
        expect(global_keyspace.table(CounterRecord.table_name).rows).to include(attributes.stringify_keys.merge('counter' => 1))
      end

      it 'should call the associated global callback' do
        record = CounterRecord.new(attributes)
        expect(GlobalCallbacks).to receive(:call).with(:record_saved, record)
        record.increment_async!(counter: 1)
      end

      #context 'when configured to use a batch' do
      #  let(:attributes) { {partition: 'Partition Key'} }
      #  let(:batch_klass) { SingleTokenCounterBatch }
      #  let(:batch) { double(:batch) }
      #  let(:bound_statement) { double(:bound_statement) }
      #
      #  subject { CounterRecord }
      #
      #  before do
      #    allow(statement).to receive(:bind).with(1, 'Partition Key').and_return(bound_statement)
      #    mock_reactor(cluster, batch_klass, {})
      #    allow(global_reactor).to receive(:perform_within_batch).with(bound_statement).and_yield(batch).and_return(Cassandra::Future.value(['OK']))
      #    subject.save_in_batch
      #  end
      #
      #  it 'should add the record to the batch' do
      #    expect(batch).to receive(:add).with(bound_statement)
      #    subject.new(attributes).increment_async!(counter: 1)
      #  end
      #end

      context 'when a consistency is specified' do
        let(:consistency) { [:quorum, :all, :one].sample }

        before { CounterRecord.write_consistency = consistency }

        it 'should increment the specified counter by the specified amount' do
          record = CounterRecord.new(attributes).increment_async!(counter: 1).get
          expect(record.execution_info).to include(consistency: consistency)
        end
      end

      it 'should not log an error' do
        expect(Logging.logger).not_to receive(:error)
        CounterRecord.new(attributes).increment_async!(counter: 1)
      end

      context 'when part of the primary key is missing' do
        let(:partition_key_types) { {part1: :text, part2: :text} }
        let(:clustering_columns_types) { {ck1: :text, ck2: :text} }
        let(:attributes) { {part1: 'Part 1', ck2: 'Does not matter', counter: 13} }
        let(:record_instance) { CounterRecord.new(attributes) }
        let(:column_values) { (counter_columns + partition_key + clustering_columns).map { |key| attributes[key] } }
        let(:record_saved_future) { record_instance.increment_async!(counter: 13)}
        let(:error_message) { 'Invalid primary key parts "part2", "ck1"' }

        subject { record_saved_future.get }

        it 'should raise an Cassandra::Invalid error' do
          expect { subject }.to raise_error(Cassandra::Errors::InvalidError, error_message)
        end

        it 'should call the associated global callback' do
          expect(GlobalCallbacks).to receive(:call).with(:save_record_failed, record_instance, a_kind_of(Cassandra::Errors::InvalidError), a_kind_of(Cassandra::Mocks::Statement), column_values)
          subject rescue nil
        end
      end

      context 'when an error occurs' do
        let(:error_message) { 'IOError: Connection Closed' }
        let(:error) { StandardError.new(error_message) }
        let(:record) { CounterRecord.new(attributes) }
        let(:column_values) { [1, *attributes.values] }

        before { allow(global_session).to receive(:execute_async).and_return(Cassandra::Future.error(error)) }

        it 'should log the error' do
          expect(Logging.logger).to receive(:error).with('Error incrementing CassandraModel::CounterRecord: IOError: Connection Closed')
          record.increment_async!(counter: 1)
        end

        it 'should execute the save record failed callback' do
          expect(GlobalCallbacks).to receive(:call).with(:save_record_failed, record, error, a_kind_of(Cassandra::Mocks::Statement), column_values)
          record.increment_async!(counter: 1)
        end

        context 'with a different error' do
          let(:error_message) { 'Error, received only 2 responses' }

          it 'should log the error' do
            expect(Logging.logger).to receive(:error).with('Error incrementing CassandraModel::CounterRecord: Error, received only 2 responses')
            CounterRecord.new(attributes).increment_async!(counter: 1)
          end
        end

        context 'with a different model' do
          it 'should log the error' do
            expect(Logging.logger).to receive(:error).with('Error incrementing CassandraModel::ImageCounter: IOError: Connection Closed')
            ImageCounter.new(attributes).increment_async!(counter: 1)
          end
        end
      end

      it 'should return the record instance' do
        record = CounterRecord.new(attributes)
        expect(record.increment_async!(counter: 2).get).to eq(record)
      end

      context 'with a different record model' do
        let(:table_name) { :image_counters }

        it 'should query the proper table' do
          ImageCounter.new(attributes).increment_async!(counter: 1).get
          expect(global_keyspace.table(ImageCounter.table_name).rows).to include(attributes.stringify_keys.merge('counter' => 1))
        end
      end

      context 'with different counter column increments' do
        it 'should increment the specified counter by the specified amount' do
          CounterRecord.new(attributes).increment_async!(counter: 2).get
          expect(global_keyspace.table(CounterRecord.table_name).rows).to include(attributes.stringify_keys.merge('counter' => 2))
        end
      end

      context 'with different counters specified' do
        let(:counter_columns) { [:counter, :additional_counter] }

        it 'should increment the specified counter by the specified amount' do
          CounterRecord.new(attributes).increment_async!(counter: 2, additional_counter: 3).get
          expect(global_keyspace.table(CounterRecord.table_name).rows).to include(attributes.stringify_keys.merge('counter' => 2, 'additional_counter' => 3))
        end
      end
    end

    describe '#increment!' do
      let(:record) { CounterRecord.new(attributes.merge(counter: 1)) }
      let(:result_future) { Cassandra::Future.value(record) }

      before do
        allow(record).to receive(:increment_async!).with(counter: 1).and_return(result_future)
      end

      it 'should delegate to #increment_async!' do
        expect(record).to receive(:increment_async!).with(counter: 1)
        record.increment!(counter: 1)
      end

      it 'should return the record' do
        expect(record.increment!(counter: 1)).to eq(record)
      end
    end

    describe '#save_async' do
      it 'should indicate that it is not implemented' do
        expect { CounterRecord.new({}).save_async }.to raise_error(NotImplementedError)
      end
    end

  end
end
