require 'rspec'

module CassandraModel
  describe CompositeRecordInstance do
    class MockRecord < Record
      extend CompositeRecordStatic
      include CompositeRecordInstance
    end

    let(:partition_key) { [:rk_model, :rk_series] }
    let(:clustering_columns) { [:ck_model] }
    let(:columns) { partition_key + clustering_columns + [:meta_data] }
    let(:query) { '' }
    let(:statement) { double(:statement) }
    let(:result_future) { MockFuture.new([]) }
    let(:connection) { double(:connection, execute_async: result_future) }
    let(:defaults) { [{model: ''}, {model: '', series: ''}] }

    before do
      MockRecord.reset!
      MockRecord.partition_key = partition_key
      MockRecord.clustering_columns = clustering_columns
      MockRecord.columns = columns
      MockRecord.composite_defaults = defaults
      allow(Record).to receive(:statement).with(query).and_return(statement)
      allow(Record).to receive(:connection).and_return(connection)
    end

    shared_examples_for 'an instance query method' do |method, params|

      subject { MockRecord.new(model: 'AABBCCDD', series: '91A', meta_data: {}) }

      describe 'resulting future' do
        let(:future) { double(:future, get: nil) }

        before do
          allow_any_instance_of(MockRecord).to receive("internal_#{method}".to_sym).and_return(future)
        end

        it 'should return a future resolving all related futures dealing with this record' do
          expect(future).to receive(:get).exactly(3).times
          subject.public_send(method, *params).get
        end

        it 'should return only the original instance' do
          expect(subject.public_send(method, *params).get).to eq(subject)
        end
      end
    end

    describe '#save_async' do
      let(:query) { 'INSERT INTO mock_records (rk_model, rk_series, ck_model, meta_data) VALUES (?, ?, ?, ?)' }

      subject { MockRecord.new(model: 'AABBCCDD', series: '91A', meta_data: {}) }

      it_behaves_like 'an instance query method', :save_async, []

      it 'should save the record with the composite columns properly resolved' do
        expect(connection).to receive(:execute_async).with(statement, 'AABBCCDD', '91A', 'AABBCCDD', {}, {})
        subject.save_async
      end

      it 'should save variations for each default column' do
        expect(connection).to receive(:execute_async).with(statement, '', '91A', 'AABBCCDD', {}, {})
        expect(connection).to receive(:execute_async).with(statement, '', '', 'AABBCCDD', {}, {})
        subject.save_async
      end
    end

    describe '#delete_async' do
      let(:query) { 'DELETE FROM mock_records WHERE rk_model = ? AND rk_series = ? AND ck_model = ?' }

      subject { MockRecord.new(model: 'AABBCCDD', series: '91A', meta_data: {}) }

      it_behaves_like 'an instance query method', :delete_async, []

      it 'should save the record with the composite columns properly resolved' do
        expect(connection).to receive(:execute_async).with(statement, 'AABBCCDD', '91A', 'AABBCCDD', {})
        subject.delete_async
      end

      it 'should save variations for each default column' do
        expect(connection).to receive(:execute_async).with(statement, '', '91A', 'AABBCCDD', {})
        expect(connection).to receive(:execute_async).with(statement, '', '', 'AABBCCDD', {})
        subject.delete_async
      end
    end

  end
end