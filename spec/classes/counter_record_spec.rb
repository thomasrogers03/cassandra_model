require 'rspec'

module CassandraModel
  describe CounterRecord do
    class CounterRecord
      def self.reset!
        @counter_columns = nil
      end
    end

    class ImageCounter < CounterRecord
    end

    let(:connection) { double(:connection) }
    let(:partition_key) { [:partition] }
    let(:counter_columns) { [:counter] }
    let(:clustering_columns) { [:cluster] }
    let(:columns) { partition_key + clustering_columns + counter_columns }
    subject { CounterRecord.new({}) }

    before do
      allow(CounterRecord).to receive(:partition_key).and_return(partition_key)
      allow(CounterRecord).to receive(:clustering_columns).and_return(clustering_columns)
      allow(CounterRecord).to receive(:columns).and_return(columns)
      allow(Record).to receive(:connection).and_return(connection)
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
      let(:results) { MockFuture.new([attributes]) }
      let(:query_result) { [QueryResult.new(attributes)] }
      let(:statement) { double(:statement) }
      let(:where_clause) { nil }
      let(:restriction) { [] }

      before do
        query = "SELECT #{counter_columns.join(', ')} FROM counter_records#{where_clause}"
        allow(CounterRecord).to receive(:statement).with(query).and_return(statement)
        allow(connection).to receive(:execute_async).with(statement, *restriction).and_return(results)
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

      context 'with different restrictions' do
        let(:where_clause) { ' WHERE partition = ? AND cluster = ?' }
        let(:restriction) { ['Partition Key', 'Cluster Key'] }

        it 'should select only the counter columns for the queried keys' do
          expect(CounterRecord.request_async(partition: 'Partition Key', cluster: 'Cluster Key').get).to eq(query_result)
        end
      end
    end

    describe '#increment_async!' do
      let(:statement) { double(:statement) }
      let(:row_key) { partition_key + clustering_columns }
      let(:where_clause) { row_key.map { |key| "#{key} = ?" }.join(' AND ') }
      let(:updated_counters) { [:counter] }
      let(:counter_clause) { updated_counters.map { |column| "#{column} = #{column} + ?" }.join(', ') }
      let(:table_name) { :counter_records }
      let(:query) { "UPDATE #{table_name} SET #{counter_clause} WHERE #{where_clause}" }
      let(:clustering_columns) { [] }
      let(:counter_columns) { [:counter, :additional_counter] }
      let(:results) { MockFuture.new([]) }

      before do
        allow(CounterRecord).to receive(:statement).with(query).and_return(statement)
        allow(connection).to receive(:execute_async).and_return(results)
      end

      it 'should increment the specified counter by the specified amount' do
        expect(connection).to receive(:execute_async).with(statement, 1, 'Partition Key')
        CounterRecord.new(partition: 'Partition Key').increment_async!(counter: 1)
      end

      it 'should return the record instance' do
        record = CounterRecord.new(partition: 'Partition Key')
        expect(record.increment_async!(counter: 2).get).to eq(record)
      end

      context 'with a different record model' do
        let(:table_name) { :image_counters }

        it 'should query the proper table' do
          expect(connection).to receive(:execute_async).with(statement, 1, 'Partition Key')
          ImageCounter.new(partition: 'Partition Key').increment_async!(counter: 1)
        end
      end

      context 'with different counter column increments' do
        it 'should increment the specified counter by the specified amount' do
          expect(connection).to receive(:execute_async).with(statement, 2, 'Partition Key')
          CounterRecord.new(partition: 'Partition Key').increment_async!(counter: 2)
        end
      end

      context 'with different counters specified' do
        let(:updated_counters) { [:counter, :additional_counter] }

        it 'should increment the specified counter by the specified amount' do
          expect(connection).to receive(:execute_async).with(statement, 2, 3, 'Partition Key')
          CounterRecord.new(partition: 'Partition Key').increment_async!(counter: 2, additional_counter: 3)
        end
      end

      context 'with different attributes' do
        let(:clustering_columns) { [:cluster] }

        it 'should increment the specified counter by the specified amount' do
          expect(connection).to receive(:execute_async).with(statement, 1, 'Other Partition Key', 'Cluster Key')
          CounterRecord.new(partition: 'Other Partition Key', cluster: 'Cluster Key').increment_async!(counter: 1)
        end
      end
    end

    describe '#save_async' do
      it 'should indicate that it is not implemented' do
        expect{CounterRecord.new({}).save_async}.to raise_error(NotImplementedError)
      end
    end

  end
end