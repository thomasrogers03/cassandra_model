require 'rspec'

module CassandraModel
  describe CompositeRecordStatic do
    class MockRecordStatic < CassandraModel::Record
      extend CompositeRecordStatic
    end

    let(:partition_key) { [:rk_model, :rk_series] }
    let(:clustering_columns) { [:ck_price, :ck_model] }
    let(:columns) { partition_key + clustering_columns + [:meta_data] }
    let(:connection) { double(:connection, request_async: MockFuture.new([])) }
    let(:statement) { double(:statement) }
    let(:query) { '' }

    before do
      MockRecordStatic.reset!
      MockRecordStatic.partition_key = partition_key
      MockRecordStatic.clustering_columns = clustering_columns
      MockRecordStatic.columns = columns
      MockRecordStatic.table_name = :mock_records
      allow(Record).to receive(:connection).and_return(connection)
      allow(Record).to receive(:statement).with(query).and_return(statement)
    end

    describe '.columns' do
      let(:columns) { [:rk_model, :series, :ck_model, :meta_data] }

      it 'should reduce the columns starting with rk_ or ck_ to base columns' do
        expect(MockRecordStatic.columns).to eq([:model, :series, :meta_data])
      end

      it 'should create methods for the reduced columns, rather than the internal ones' do
        record = MockRecordStatic.new({})
        record.model = 'KKBBCD'
        expect(record.model).to eq('KKBBCD')
      end

      context 'with a different set of columns' do
        let(:columns) { [:rk_model, :rk_series, :rk_colour, :ck_price, :ck_model, :ck_colour, :meta_data] }

        it 'should reduce the columns starting with rk_ or ck_ to base columns' do
          expect(MockRecordStatic.columns).to eq([:model, :series, :colour, :price, :meta_data])
        end
      end
    end

    shared_examples_for 'a composite column map' do |method, prefix|
      describe ".#{method}" do
        let(:columns) { [:rk_model, :rk_series, :rk_colour, :ck_price, :ck_model, :ck_colour, :meta_data] }

        before { MockRecordStatic.columns }

        {"#{prefix}_model".to_sym => :model, "#{prefix}_colour".to_sym => :colour}.each do |actual, composite|
          it 'should map a reduced row key to its original name' do
            expect(MockRecordStatic.public_send(method)[composite]).to eq(actual)
          end

          it 'should map a the original row key to its reduced name' do
            expect(MockRecordStatic.public_send(method)[actual]).to eq(composite)
          end
        end
      end
    end

    it_behaves_like 'a composite column map', :composite_pk_map, :rk
    it_behaves_like 'a composite column map', :composite_ck_map, :ck

    describe '.composite_defaults' do
      let(:columns) { [:rk_model, :rk_series, :rk_colour, :ck_price, :ck_model, :ck_colour, :meta_data] }
      let(:defaults) { nil }

      before do
        MockRecordStatic.columns
        MockRecordStatic.composite_defaults = defaults
      end

      subject { MockRecordStatic.composite_defaults }

      it { is_expected.to be_nil }

      context 'with default values for composite row keys' do
        let(:defaults) { [{model: ''}] }

        it 'should map the internal columns to their default values' do
          is_expected.to eq([{rk_model: ''}])
        end

        context 'with multiple variations' do
          let(:defaults) { [{model: ''}, {model: '', series: ''}, {series: '', colour: ''}] }

          it 'should map all the internal columns for each variation' do
            is_expected.to eq([{rk_model: ''}, {rk_model: '', rk_series: ''}, {rk_series: '', rk_colour: ''}])
          end
        end
      end
    end

    describe '.request_async' do
      let(:query) { 'SELECT * FROM mock_records WHERE rk_model = ? AND rk_series = ? AND ck_price = ?' }
      let(:defaults) { [{model: ''}, {model: '', series: ''}] }

      before do
        MockRecordStatic.columns
        MockRecordStatic.composite_defaults = defaults
      end

      it 'should query by mapping composite columns to the real ones' do
        expect(connection).to receive(:execute_async).with(statement, 'AABBCCDD', '91A', 9.99, {})
        MockRecordStatic.request_async(model: 'AABBCCDD', series: '91A', price: 9.99)
      end

      context 'when missing information from the query' do
        let(:query) { 'SELECT * FROM mock_records WHERE rk_series = ? AND ck_price = ? AND rk_model = ?' }

        it 'should add the default values to the query' do
          expect(connection).to receive(:execute_async).with(statement, '91A', 9.99, '', {})
          MockRecordStatic.request_async(series: '91A', price: 9.99)
        end

        context 'with different missing information' do
          let(:query) { 'SELECT * FROM mock_records WHERE ck_price = ? AND rk_model = ? AND rk_series = ?' }

          it 'should add the default values to the query for all default variations' do
            expect(connection).to receive(:execute_async).with(statement, 9.99, '', '', {})
            MockRecordStatic.request_async(price: 9.99)
          end
        end
      end
    end

  end
end