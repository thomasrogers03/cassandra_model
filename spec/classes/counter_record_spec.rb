require 'rspec'

module CassandraModel
  describe CounterRecord do
    class ImageCounter < CounterRecord
    end

    let(:partition_key) { [:partition] }
    let(:counter_columns) { [:counter] }
    let(:clustering_columns) { [:cluster] }
    let(:columns) { partition_key + clustering_columns + counter_columns }
    subject { CounterRecord.new({}) }

    before do
      mock_simple_table(:counter_records, partition_key, clustering_columns, counter_columns)
      mock_simple_table(:image_counters, partition_key, clustering_columns, counter_columns)
      allow(Logging.logger).to receive(:error)
    end

    after do
      CounterRecord.reset!
      ImageCounter.reset!
    end

    it { should be_a_kind_of(Record) }

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
      let(:row_key) { partition_key + clustering_columns }
      let(:where_clause) { row_key.map { |key| "#{key} = ?" }.join(' AND ') }
      let(:updated_counters) { [:counter] }
      let(:counter_clause) { updated_counters.map { |column| "#{column} = #{column} + ?" }.join(', ') }
      let(:table_name) { :counter_records }
      let(:query) { "UPDATE #{table_name} SET #{counter_clause} WHERE #{where_clause}" }
      let!(:statement) { mock_prepare(query) }
      let(:clustering_columns) { [] }
      let(:counter_columns) { [:counter, :additional_counter] }
      let(:future_error) { nil }
      let(:results) { MockFuture.new(result: [], error: future_error) }
      let(:attributes) { {partition: 'Partition Key'} }

      before do
        allow(CounterRecord).to receive(:statement).with(query).and_return(statement)
        allow(connection).to receive(:execute_async).and_return(results)
      end

      it 'should increment the specified counter by the specified amount' do
        expect(connection).to receive(:execute_async).with(statement, 1, 'Partition Key', {})
        CounterRecord.new(attributes).increment_async!(counter: 1)
      end

      it 'should call the associated global callback' do
        record = CounterRecord.new(attributes)
        expect(GlobalCallbacks).to receive(:call).with(:record_saved, record)
        record.increment_async!(counter: 1)
      end

      context 'when configured to use a batch' do
        let(:attributes) { {partition: 'Partition Key'} }
        let(:batch_klass) { SingleTokenCounterBatch }
        let(:batch) { double(:batch) }
        let(:bound_statement) { double(:bound_statement) }

        subject { CounterRecord }

        before do
          allow(statement).to receive(:bind).with(1, 'Partition Key').and_return(bound_statement)
          mock_reactor(cluster, batch_klass, {})
          allow(global_reactor).to receive(:perform_within_batch).with(bound_statement).and_yield(batch).and_return(Cassandra::Future.value(['OK']))
          subject.save_in_batch
        end

        it 'should add the record to the batch' do
          expect(batch).to receive(:add).with(bound_statement)
          subject.new(attributes).increment_async!(counter: 1)
        end
      end

      context 'when a consistency is specified' do
        let(:consistency) { :quorum }

        before { CounterRecord.write_consistency = consistency }

        it 'should increment the specified counter by the specified amount' do
          expect(connection).to receive(:execute_async).with(statement, 1, 'Partition Key', consistency: consistency)
          CounterRecord.new(partition: 'Partition Key').increment_async!(counter: 1)
        end

        context 'with a different consistency' do
          let(:consistency) { :all }

          it 'should increment the specified counter by the specified amount' do
            expect(connection).to receive(:execute_async).with(statement, 1, 'Partition Key', consistency: consistency)
            CounterRecord.new(partition: 'Partition Key').increment_async!(counter: 1)
          end
        end
      end

      it 'should not log an error' do
        expect(Logging.logger).not_to receive(:error)
        CounterRecord.new(partition: 'Partition Key').increment_async!(counter: 1)
      end

      context 'when an error occurs' do
        let(:future_error) { 'IOError: Connection Closed' }
        let(:record) { CounterRecord.new(partition: 'Partition Key') }

        it 'should log the error' do
          expect(Logging.logger).to receive(:error).with('Error incrementing CassandraModel::CounterRecord: IOError: Connection Closed')
          record.increment_async!(counter: 1)
        end

        it 'should execute the save record failed callback' do
          expect(GlobalCallbacks).to receive(:call).with(:save_record_failed, record, future_error)
          record.increment_async!(counter: 1)
        end

        context 'with a different error' do
          let(:future_error) { 'Error, received only 2 responses' }

          it 'should log the error' do
            expect(Logging.logger).to receive(:error).with('Error incrementing CassandraModel::CounterRecord: Error, received only 2 responses')
            CounterRecord.new(partition: 'Partition Key').increment_async!(counter: 1)
          end
        end

        context 'with a different model' do
          it 'should log the error' do
            expect(Logging.logger).to receive(:error).with('Error incrementing CassandraModel::ImageCounter: IOError: Connection Closed')
            ImageCounter.new(partition: 'Partition Key').increment_async!(counter: 1)
          end
        end
      end

      it 'should return the record instance' do
        record = CounterRecord.new(partition: 'Partition Key')
        expect(record.increment_async!(counter: 2).get).to eq(record)
      end

      context 'with a different record model' do
        let(:table_name) { :image_counters }

        it 'should query the proper table' do
          expect(connection).to receive(:execute_async).with(statement, 1, 'Partition Key', {})
          ImageCounter.new(partition: 'Partition Key').increment_async!(counter: 1)
        end
      end

      context 'with different counter column increments' do
        it 'should increment the specified counter by the specified amount' do
          expect(connection).to receive(:execute_async).with(statement, 2, 'Partition Key', {})
          CounterRecord.new(partition: 'Partition Key').increment_async!(counter: 2)
        end
      end

      context 'with different counters specified' do
        let(:updated_counters) { [:counter, :additional_counter] }

        it 'should increment the specified counter by the specified amount' do
          expect(connection).to receive(:execute_async).with(statement, 2, 3, 'Partition Key', {})
          CounterRecord.new(partition: 'Partition Key').increment_async!(counter: 2, additional_counter: 3)
        end
      end

      context 'with different attributes' do
        let(:clustering_columns) { [:cluster] }

        it 'should increment the specified counter by the specified amount' do
          expect(connection).to receive(:execute_async).with(statement, 1, 'Other Partition Key', 'Cluster Key', {})
          CounterRecord.new(partition: 'Other Partition Key', cluster: 'Cluster Key').increment_async!(counter: 1)
        end
      end
    end

    describe '#increment!' do
      let(:record) { CounterRecord.new(partition: 'Other Partition Key', cluster: 'Cluster Key') }
      let(:result_future) { MockFuture.new(result: record) }

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
