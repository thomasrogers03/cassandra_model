require 'rspec'

module CassandraModel
  describe CompositeRecordInstance do
    class MockRecordInstance < Record
      include CompositeRecordInstance
    end

    let(:partition_key) { [:rk_model, :rk_series] }
    let(:clustering_columns) { [:ck_model] }
    let(:remaining_columns) { [:meta_data] }
    let(:query) { '' }
    let!(:statement) { mock_prepare(query) }
    let(:result_future) { MockFuture.new([]) }
    let(:defaults) { [{model: ''}, {model: '', series: ''}] }

    subject { MockRecordInstance.new(model: 'AABBCCDD', series: '91A', meta_data: {}) }

    before do
      MockRecordInstance.reset!
      MockRecordInstance.table_name = :mock_records
      MockRecordInstance.composite_defaults = defaults
      mock_simple_table(:mock_records, partition_key, clustering_columns, remaining_columns)
    end

    shared_examples_for 'an instance query method' do |method, params|

      describe 'resulting future' do
        let(:future) { double(:future, get: nil) }

        before do
          args = params.empty? ? [no_args] : params
          allow_any_instance_of(MockRecordInstance).to receive("internal_#{method}".to_sym).with(*args).and_return(future)
        end

        it 'should return a future resolving all related futures dealing with this record' do
          expect(future).to receive(:get).exactly(3).times
          subject.public_send(method, *params).get
        end

        it 'should return only the original instance' do
          expect(subject.public_send(method, *params).get).to eq(subject)
        end

        it 'should set a leader when wrapping multiple futures' do
          expect(future).to receive(:on_success).once
          subject.public_send(method, *params).on_success {  }
        end
      end
    end

    describe '#save_async' do
      let(:query) { 'INSERT INTO mock_records (rk_model, rk_series, ck_model, meta_data) VALUES (?, ?, ?, ?)' }

      it_behaves_like 'an instance query method', :save_async, [check_exists: true]

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

    describe '#update_async' do
      let(:query) { 'UPDATE mock_records SET meta_data = ? WHERE rk_model = ? AND rk_series = ? AND ck_model = ?' }
      attributes = {meta_data: {'Description' => 'A powerful drill'}}

      it_behaves_like 'an instance query method', :update_async, [attributes]

      it 'should save the record with the composite columns properly resolved' do
        expect(connection).to receive(:execute_async).with(statement, {'Description' => 'A powerful drill'}, 'AABBCCDD', '91A', 'AABBCCDD', {})
        subject.update_async(attributes)
      end

      it 'should save variations for each default column' do
        expect(connection).to receive(:execute_async).with(statement, {'Description' => 'A powerful drill'}, '', '91A', 'AABBCCDD', {})
        expect(connection).to receive(:execute_async).with(statement, {'Description' => 'A powerful drill'}, '', '', 'AABBCCDD', {})
        subject.update_async(attributes)
      end
    end

  end
end