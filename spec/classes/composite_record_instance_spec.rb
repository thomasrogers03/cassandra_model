require 'rspec'

module CassandraModel
  describe CompositeRecordInstance do
    class MockRecord < Record
      extend CompositeRecordStatic
      include CompositeRecordInstance
    end

    let(:columns) { [] }
    let(:query) { '' }
    let(:statement) { double(:statement) }
    let(:result_future) { MockFuture.new([]) }
    let(:connection) { double(:connection, execute_async: result_future) }

    before do
      MockRecord.reset!
      MockRecord.columns = columns
      allow(Record).to receive(:statement).with(query).and_return(statement)
      allow(Record).to receive(:connection).and_return(connection)
    end

    describe '#save_async' do
      let(:columns) { [:rk_model, :rk_series, :ck_model, :meta_data] }
      let(:query) { 'INSERT INTO mock_records (rk_model, rk_series, ck_model, meta_data) VALUES (?, ?, ?, ?)' }
      let(:defaults) { [{model: ''}, {model: '', series: ''}] }

      before { MockRecord.composite_defaults = defaults }
      subject { MockRecord.new(model: 'AABBCCDD', series: '91A', meta_data: {}) }

      it 'should save the record with the composite columns properly resolved' do
        expect(connection).to receive(:execute_async).with(statement, 'AABBCCDD', '91A', 'AABBCCDD', {}, {})
        subject.save_async
      end

      it 'should save variations for each default column' do
        expect(connection).to receive(:execute_async).with(statement, '', '91A', 'AABBCCDD', {}, {})
        expect(connection).to receive(:execute_async).with(statement, '', '', 'AABBCCDD', {}, {})
        subject.save_async
      end

      describe 'resulting future' do
        let(:future) { double(:future, get: nil) }

        before do
          allow_any_instance_of(MockRecord).to receive(:internal_save_async).and_return(future)
        end

        it 'should return a future resolving all related futures saving this record' do
          expect(future).to receive(:get).exactly(3).times
          subject.save_async.get
        end

        it 'should return only the original instance' do
          expect(subject.save_async.get).to eq(subject)
        end
      end
    end
  end
end