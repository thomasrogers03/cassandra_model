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
      allow(CounterRecord).to receive(:partition_key).and_return(partition_key)
      allow(CounterRecord).to receive(:clustering_columns).and_return(clustering_columns)
      allow(CounterRecord).to receive(:columns).and_return(columns)
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
        expect(CounterRecord).not_to receive(:partition_key)
        expect(CounterRecord).not_to receive(:clustering_columns)
        expect(CounterRecord).not_to receive(:columns)
        CounterRecord.counter_columns
      end

      context 'with a different counter columns' do
        let(:counter_columns) { [:different_counter, :extra_counter] }

        it 'should be the columns not part of the partition or clustering keys' do
          expect(CounterRecord.counter_columns).to eq(counter_columns)
        end
      end
    end

    describe '.request_async' do
      let(:attributes) { {cluster: 6} }
      let(:page_results) { ['partition' => 'Partition Key'] }
      let(:result_page) { MockPage.new(true, MockFuture.new([]), [attributes]) }
      let(:results) { MockFuture.new(result_page) }
      let(:query_result) { [QueryResult.new(attributes)] }
      let(:query) { "SELECT #{counter_columns.join(', ')} FROM counter_records#{where_clause}" }
      let!(:statement) { mock_prepare(query) }
      let(:where_clause) { nil }
      let(:restriction) { [] }

      before do
        allow(connection).to receive(:execute_async).with(statement, *restriction, {}).and_return(results)
      end

      it 'should select only the counter columns' do
        expect(CounterRecord.request_async({}).get).to eq(query_result)
      end

      context 'with different counter columns' do
        let(:counter_columns) { [:different_counter, :extra_counter] }

        it 'should select only the counter columns' do
          expect(CounterRecord.request_async({}).get).to eq(query_result)
        end
      end

      context 'when options are provided' do
        it 'should pass the options to the underlying request' do
          expect(Record).to receive(:request_async).with({}, hash_including(limit: 10))
          CounterRecord.request_async({}, limit: 10)
        end

        context 'when selecting additional columns' do
          it 'should include those columns along with the count' do
            expect(Record).to receive(:request_async).with({}, hash_including(select: partition_key + counter_columns))
            CounterRecord.request_async({}, select: partition_key)
          end
        end

        context 'when selecting the counter column' do
          it 'should only specify to select it once' do
            expect(Record).to receive(:request_async).with({}, hash_including(select: counter_columns))
            CounterRecord.request_async({}, select: counter_columns)
          end
        end
      end

      context 'with different restrictions' do
        let(:where_clause) { ' WHERE partition = ? AND cluster = ?' }
        let(:restriction) { ['Partition Key', 'Cluster Key'] }

        it 'should select only the counter columns for the queried keys' do
          expect(CounterRecord.request_async(partition: 'Partition Key', cluster: 'Cluster Key').get).to eq(query_result)
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

      before do
        allow(CounterRecord).to receive(:statement).with(query).and_return(statement)
        allow(connection).to receive(:execute_async).and_return(results)
      end

      it 'should increment the specified counter by the specified amount' do
        expect(connection).to receive(:execute_async).with(statement, 1, 'Partition Key', {})
        CounterRecord.new(partition: 'Partition Key').increment_async!(counter: 1)
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

        it 'should log the error' do
          expect(Logging.logger).to receive(:error).with('Error incrementing CassandraModel::CounterRecord: IOError: Connection Closed')
          CounterRecord.new(partition: 'Partition Key').increment_async!(counter: 1)
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