require 'rspec'

module CassandraModel
  describe CompositeRecordStatic do
    class MockRecordStatic < CassandraModel::Record
      extend CompositeRecordStatic
    end

    let(:partition_key) { [:rk_model, :rk_series] }
    let(:clustering_columns) { [:ck_price, :ck_model] }
    let(:remaining_columns) { [:meta_data] }
    let(:query) { '' }
    let!(:statement) { mock_prepare(query) }

    before do
      MockRecordStatic.reset!
      MockRecordStatic.table_name = :mock_records
      mock_simple_table(:mock_records, partition_key, clustering_columns, remaining_columns)
    end

    describe '.columns' do
      let(:partition_key) { [:rk_model, :series] }
      let(:clustering_columns) { [:ck_model] }

      it 'should reduce the columns starting with rk_ or ck_ to base columns' do
        expect(MockRecordStatic.columns).to eq([:model, :series, :meta_data])
      end

      it 'should create methods for the reduced columns, rather than the internal ones' do
        record = MockRecordStatic.new({})
        record.model = 'KKBBCD'
        expect(record.model).to eq('KKBBCD')
      end

      context 'with a different set of columns' do
        let(:partition_key) { [:rk_model, :rk_series, :rk_colour] }
        let(:clustering_columns) { [:ck_price, :ck_model] }

        it 'should reduce the columns starting with rk_ or ck_ to base columns' do
          expect(MockRecordStatic.columns).to eq([:model, :series, :colour, :price, :meta_data])
        end
      end
    end

    shared_examples_for 'a composite column map' do |method, prefix|
      describe ".#{method}" do
        let(:partition_key) { [:rk_model, :rk_series, :rk_colour] }
        let(:clustering_columns) { [:ck_price, :ck_model, :ck_colour] }

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
      let(:partition_key) { [:rk_model, :rk_series, :rk_colour] }
      let(:clustering_columns) { [:ck_price, :ck_model, :ck_colour] }
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

    describe '.generate_composite_defaults' do
      let(:column_defaults) { {model: '', series: ''} }
      let(:truth_table) { [[:model]] }

      subject { MockRecordStatic.composite_defaults }

      before { MockRecordStatic.generate_composite_defaults(column_defaults, truth_table) }

      it 'should should generate a table of composite defaults given default column mapping and a truth table' do
        is_expected.to eq([{rk_series: ''}])
      end

      context 'with a different truth table' do
        let(:truth_table) { [[:model], []] }
        it { is_expected.to eq [{rk_series: ''}, {rk_model: '', rk_series: ''}] }
      end
    end

    describe '.generate_composite_defaults_from_inquirer' do
      let(:inquirer) { DataInquirer.new }
      let(:partition_key) { {title: 'NO TITLE', series: 'NULL', year: 1900} }
      let(:first_inquiry) { [:title, :series, :year] }
      let(:second_inquiry) { [:series, :year] }

      subject { MockRecordStatic.composite_defaults }

      before do
        inquirer.knows_about(*first_inquiry)
        inquirer.knows_about(*second_inquiry)
        partition_key.each { |key, value| inquirer.defaults(key).to(value) }
        MockRecordStatic.generate_composite_defaults_from_inquirer(inquirer)
      end

      it 'should generate a table of composite defaults from the data set inquirer' do
        is_expected.to eq([rk_title: 'NO TITLE'])
      end

      context 'with a different inquiry' do
        let(:partition_key) { {make: '', model: 'NULL', year: 0} }
        let(:first_inquiry) { [:make, :model] }
        let(:second_inquiry) { [:year] }

        it 'should generate a table of composite defaults from the data set inquirer' do
          is_expected.to eq([{rk_year: 0}, {rk_make: '', rk_model: 'NULL'}])
        end
      end
    end

    describe '.request_async' do
      let(:query) { 'SELECT * FROM mock_records WHERE rk_model = ? AND rk_series = ? AND ck_price = ?' }
      let(:defaults) { [{model: ''}, {model: '', series: ''}] }

      before do
        MockRecordStatic.composite_defaults = defaults
      end

      it 'should query by mapping composite columns to the real ones' do
        expect(connection).to receive(:execute_async).with(statement, 'AABBCCDD', '91A', 9.99, {})
        MockRecordStatic.request_async(model: 'AABBCCDD', series: '91A', price: 9.99)
      end

      context 'when a composite column is part of the remaining columns' do
        let(:partition_key) { [:rk_model, :rk_series] }
        let(:clustering_columns) { [:ck_series] }
        let(:remaining_columns) { [] }
        let(:defaults) { [{model: ''}] }
        let(:results) { [['rk_model' => '', 'rk_series' => '91A', 'ck_series' => '91A', 'model' => 'EEFFGG']] }
        let(:query) { 'SELECT * FROM mock_records WHERE rk_series = ? AND rk_model = ?' }
        let(:execute_params) { [statement, '91A', '', {}] }

        before { mock_query_result(execute_params, results) }

        it 'should not re-map the composite column to the real one' do
          record = MockRecordStatic.request_async(series: '91A').get.first
          expect(record.attributes).not_to include(:rk_model)
        end
      end

      context 'when selecting composite columns' do
        let(:query) { 'SELECT ck_model FROM mock_records WHERE rk_model = ? AND rk_series = ? AND ck_price = ?' }

        it 'should map the composite column to the clustering column' do
          expect(connection).to receive(:execute_async).with(statement, 'AABBCCDD', '91A', 9.99, {})
          MockRecordStatic.request_async({model: 'AABBCCDD', series: '91A', price: 9.99}, select: [:model])
        end

        context 'with a different columns selected' do
          let(:query) { 'SELECT ck_model, rk_series, meta_data FROM mock_records WHERE rk_model = ? AND rk_series = ? AND ck_price = ?' }

          it 'should map the composite column to the clustering column' do
            expect(connection).to receive(:execute_async).with(statement, 'AABBCCDD', '91A', 9.99, {})
            MockRecordStatic.request_async({model: 'AABBCCDD', series: '91A', price: 9.99}, select: [:model, :series, :meta_data])
          end
        end
      end

      context 'when missing information from the query' do
        let(:query) { 'SELECT * FROM mock_records WHERE rk_series = ? AND ck_price = ? AND rk_model = ?' }

        it 'should add the default values to the query' do
          expect(connection).to receive(:execute_async).with(statement, '91A', 9.99, '', {})
          MockRecordStatic.request_async(series: '91A', price: 9.99)
        end

        context 'when a field has the same name as a composite column' do
          let(:partition_key) { [:rk_model, :rk_series] }
          let(:clustering_columns) { [:ck_series] }
          let(:remaining_columns) { [:model] }
          let(:query) { 'SELECT model FROM mock_records WHERE rk_series = ? AND rk_model = ?' }

          it 'should query for the field' do
            expect(connection).to receive(:execute_async).with(statement, '91A', '', {})
            MockRecordStatic.request_async({series: '91A'}, select: [:model])
          end
        end

        context 'with different missing information' do
          let(:page_results) do
            [
                {
                    'rk_model' => '',
                    'rk_series' => '91C',
                    'ck_price' => 9.99,
                    'ck_model' => 'AABBCCDD',
                    'meta_data' => nil
                },
                {
                    'rk_model' => '',
                    'rk_series' => '91C',
                    'ck_price' => 9.99,
                    'ck_model' => 'DDEEFFGG',
                    'meta_data' => nil
                },
            ]
          end
          let(:result) { MockPage.new(true, nil, page_results) }
          let(:result_future) { MockFuture.new(result) }
          let(:query) { 'SELECT * FROM mock_records WHERE ck_price = ? AND rk_model = ? AND rk_series = ?' }

          before { allow(connection).to receive(:execute_async).with(statement, 9.99, '', '', {}).and_return(result_future) }

          it 'should add the default values to the query for all default variations' do
            expect(connection).to receive(:execute_async).with(statement, 9.99, '', '', {})
            MockRecordStatic.request_async(price: 9.99)
          end

          context 'when the composite defaults are in a different order than the partition key' do
            let(:query) { 'SELECT * FROM mock_records WHERE ck_price = ? AND rk_series = ? AND rk_model = ?' }
            let(:defaults) { [{model: ''}, {series: '', model: ''}] }

            it 'should add the default values to the query for all default variations' do
              expect(connection).to receive(:execute_async).with(statement, 9.99, '', '', {})
              MockRecordStatic.request_async(price: 9.99)
            end
          end

          it 'should map the real columns to composite ones' do
            results = MockRecordStatic.request_async(price: 9.99).get
            expect(results).to eq([
                                      MockRecordStatic.new(model: 'AABBCCDD', series: '91C', price: 9.99, meta_data: nil),
                                      MockRecordStatic.new(model: 'DDEEFFGG', series: '91C', price: 9.99, meta_data: nil)
                                  ])
          end

          context 'when selecting specific columns' do
            let(:page_results) { [{'ck_model' => 'DDEEFFGG'}] }
            let(:query) { 'SELECT ck_model FROM mock_records WHERE ck_price = ? AND rk_model = ? AND rk_series = ?' }

            it 'should map only the selected composite columns' do
              results = MockRecordStatic.request_async({price: 9.99}, select: [:model]).get
              expect(results).to eq([QueryResult.create(model: 'DDEEFFGG')])
            end
          end
        end
      end
    end

  end
end