require 'rspec'

module CassandraModel
  describe CompositeRecord do
    class MockRecordInstance < Record
      include CompositeRecord

      protected

      # so that our NoMethodError test fails
      def internal_increment_async!(*)
        Cassandra::Future.value(nil)
      end
    end

    class MockCounterRecordInstance < CounterRecord
      include CompositeRecord
    end

    let(:record_klass) { MockRecordInstance }
    let(:partition_key) { [:rk_model, :rk_series] }
    let(:clustering_columns) { [:ck_model] }
    let(:remaining_columns) { [:meta_data] }
    let(:query) { '' }
    let!(:statement) { mock_prepare(query) }
    let(:result_future) { MockFuture.new([]) }
    let(:defaults) { [{model: ''}, {model: '', series: ''}] }

    subject { record_klass.new(model: 'AABBCCDD', series: '91A', meta_data: {}) }

    before do
      record_klass.reset!
      record_klass.table_name = :mock_records
      record_klass.composite_defaults = defaults
      mock_simple_table(:mock_records, partition_key, clustering_columns, remaining_columns)
    end

    shared_examples_for 'an instance query method' do |method, params|

      describe 'resulting future' do
        let(:future) { MockFuture.new(nil) }

        before do
          args = params.empty? ? [no_args] : params
          allow_any_instance_of(record_klass).to receive("internal_#{method}".to_sym).with(*args).and_return(future)
        end

        it 'should return a future resolving all related futures dealing with this record' do
          expect(future).to receive(:on_complete).exactly(3).times.and_call_original
          subject.public_send(method, *params).get
        end

        it 'should return only the original instance' do
          expect(subject.public_send(method, *params).get).to eq(subject)
        end

        it 'should return the original record on completion' do
          subject.public_send(method, *params).on_success { |result| expect(result).to eq(subject) }
        end
      end
    end

    describe '#save_async' do
      let(:query) { 'INSERT INTO mock_records (rk_model, rk_series, ck_model, meta_data) VALUES (?, ?, ?, ?)' }

      it_behaves_like 'an instance query method', :save_async, [check_exists: true]

      it 'should save the record with the composite columns properly resolved' do
        expect(connection).to receive(:execute_async).with(statement, 'AABBCCDD', '91A', 'AABBCCDD', {}, {})
        subject.save_async.get
      end

      it 'should save variations for each default column' do
        expect(connection).to receive(:execute_async).with(statement, '', '91A', 'AABBCCDD', {}, {})
        expect(connection).to receive(:execute_async).with(statement, '', '', 'AABBCCDD', {}, {})
        subject.save_async.get
      end

      context 'with a deferred column' do
        let(:fake_column_value) { [:some, great: 'stuff'] }
        let(:serialized_fake_column) { Marshal.dump(fake_column_value) }
        subject { record_klass.new(model: 'AABBCCDD', series: '91A', fake_column: fake_column_value) }

        before do
          record_klass.deferred_column :fake_column, on_load: ->(attributes) { Marshal.load(attributes[:meta_data]) if attributes[:meta_data] },
                                       on_save: ->(attributes, value) { attributes[:meta_data] = Marshal.dump(value) }
          subject.fake_column
        end
        after { record_klass.send(:remove_method, :fake_column) if record_klass.instance_methods(false).include?(:fake_column) }

        it 'should save variations for each default column' do
          expect(connection).to receive(:execute_async).with(statement, '', '91A', 'AABBCCDD', serialized_fake_column, {})
          expect(connection).to receive(:execute_async).with(statement, '', '', 'AABBCCDD', serialized_fake_column, {})
          subject.save_async.get
        end
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

    describe '#increment_async!' do
      let(:record_klass) { MockCounterRecordInstance }
      let(:query) { 'UPDATE mock_records SET meta_data = meta_data + ? WHERE rk_model = ? AND rk_series = ? AND ck_model = ?' }

      it_behaves_like 'an instance query method', :increment_async!, [meta_data: 7]

      it 'should save the record with the composite columns properly resolved' do
        expect(connection).to receive(:execute_async).with(statement, 16, 'AABBCCDD', '91A', 'AABBCCDD', {})
        subject.increment_async!(meta_data: 16)
      end

      it 'should save variations for each default column' do
        expect(connection).to receive(:execute_async).with(statement, 73, '', '91A', 'AABBCCDD', {})
        expect(connection).to receive(:execute_async).with(statement, 73, '', '', 'AABBCCDD', {})
        subject.increment_async!(meta_data: 73)
      end

      context 'when not a counter record' do
        let(:record_klass) { MockRecordInstance }

        it 'should not define the method' do
          expect { subject.increment_async!(meta_data: 73) }.to raise_error(NoMethodError)
        end
      end
    end

  end
end
