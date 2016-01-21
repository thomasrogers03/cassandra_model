require 'rspec'

module CassandraModel
  describe CompositeRecordStatic do
    class MockRecordStatic < CassandraModel::Record
      extend CompositeRecordStatic

      def validate_attributes!(attributes)
      end

      def attribute(column)
        attributes[column] ||
            attributes[self.class.composite_ck_map[column]] ||
            attributes[self.class.composite_pk_map[column]]
      end

      def internal_attributes
        internal_columns.inject({}) do |memo, column|
          memo.merge(column => attribute(column))
        end
      end
    end

    let(:partition_key) { [:rk_model, :rk_series] }
    let(:clustering_columns) { [:ck_price, :ck_model] }
    let(:remaining_columns) { [:meta_data] }
    let(:columns) { partition_key + clustering_columns + remaining_columns }
    let(:query) { '' }
    let!(:statement) { mock_prepare(query) }

    before do
      MockRecordStatic.reset!
      MockRecordStatic.table_name = :mock_records
      mock_simple_table(:mock_records, partition_key, clustering_columns, columns)
    end

    describe 'column name methods' do
      let(:partition_key) { [:rk_model, :series] }
      let(:clustering_columns) { [:ck_model] }

      describe '.columns' do
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

      describe '.denormalized_column_map' do
        let(:input_columns) { MockRecordStatic.internal_columns }
        let(:expected_map) { {rk_model: :rk_model, series: :series, ck_model: :ck_model, meta_data: :meta_data} }

        subject { MockRecordStatic.denormalized_column_map(input_columns) }

        it { is_expected.to eq(expected_map) }

        context 'when the input columns have been normalized' do
          let(:input_columns) { MockRecordStatic.columns }
          let(:expected_map) { {rk_model: :model, series: :series, ck_model: :model, meta_data: :meta_data} }

          it { is_expected.to eq(expected_map) }

          context 'when a column is not available in the input' do
            let(:input_columns) { [:model] }
            let(:expected_map) { {rk_model: :model, ck_model: :model} }

            it { is_expected.to eq(expected_map) }
          end
        end
      end

      describe '.partition_key' do
        subject { MockRecordStatic.partition_key }

        it { is_expected.to eq([:model, :series]) }

        context 'with a different set of columns' do
          let(:partition_key) { [:rk_model_data, :rk_series_number, :rk_colour] }
          it { is_expected.to eq([:model_data, :series_number, :colour]) }
        end
      end

      describe '.clustering_columns' do
        subject { MockRecordStatic.clustering_columns }

        it { is_expected.to eq([:model]) }

        context 'with a different set of columns' do
          let(:clustering_columns) { [:ck_min_price, :max_price] }
          it { is_expected.to eq([:min_price, :max_price]) }
        end
      end

      describe '.primary_key' do
        subject { MockRecordStatic.primary_key }

        it { is_expected.to eq([:model, :series]) }

        context 'with a different set of columns' do
          let(:partition_key) { [:rk_model_data, :rk_series_number, :rk_colour] }
          let(:clustering_columns) { [:ck_min_price, :max_price] }
          it { is_expected.to eq([:model_data, :series_number, :colour, :min_price, :max_price]) }
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
      let(:partition_key) { inquirer_columns.keys.map { |column| :"rk_#{column}" } }
      let(:clustering_columns) { inquirer_columns.keys.map { |column| :"ck_#{column}" } }
      let(:inquirer) { DataInquirer.new }
      let(:inquirer_columns) { {title: 'NO TITLE', series: 'NULL', year: 1900} }
      let(:first_inquiry) { [:title, :series, :year] }
      let(:second_inquiry) { [:series, :year] }

      subject { MockRecordStatic.composite_defaults }

      before do
        inquirer.knows_about(*first_inquiry)
        inquirer.knows_about(*second_inquiry)
        inquirer_columns.each { |key, value| inquirer.defaults(key).to(value) }
        MockRecordStatic.generate_composite_defaults_from_inquirer(inquirer)
      end

      it 'should generate a table of composite defaults from the data set inquirer' do
        is_expected.to eq([rk_title: 'NO TITLE'])
      end

      context 'with a different inquiry' do
        let(:inquirer_columns) { {make: '', model: 'NULL', year: 0} }
        let(:first_inquiry) { [:make, :model] }
        let(:second_inquiry) { [:year] }

        it 'should generate a table of composite defaults from the data set inquirer' do
          is_expected.to eq([{rk_year: 0}, {rk_make: '', rk_model: 'NULL'}])
        end
      end
    end

    describe '.restriction_attributes' do
      let(:defaults) { [{model: ''}, {model: '', series: ''}] }
      let(:restriction) { {model: 'AABBCCDD', series: '91A', price: 9.99} }

      subject { MockRecordStatic.restriction_attributes(restriction) }

      before do
        MockRecordStatic.composite_defaults = defaults
      end

      it { is_expected.to eq(rk_model: 'AABBCCDD', rk_series: '91A', ck_price: 9.99) }

      context 'when missing information from the restriction' do
        let(:restriction) { {series: '91A', price: 9.99} }

        it { is_expected.to eq(rk_series: '91A', ck_price: 9.99, rk_model: '') }
      end
    end

    describe '.select_columns' do
      let(:partition_key) { [:rk_model] }
      let(:clustering_columns) { [:ck_series] }
      let(:remaining_columns) { [:model, :series, :meta_data] }
      let(:select_columns) { [:model, :series] }

      subject { MockRecordStatic.select_columns(select_columns) }

      it { is_expected.to eq(select_columns) }

      context 'with different columns to be selected' do
        let(:select_columns) { [:series, :meta_data] }

        it { is_expected.to eq(select_columns) }
      end

      context 'when a selected column is only present as a clustering key part' do
        let(:remaining_columns) { [:model, :meta_data] }

        it { is_expected.to eq([:model, :ck_series]) }
      end

      context 'when a selected column is only present as a partition key part' do
        let(:remaining_columns) { [:series, :meta_data] }

        it { is_expected.to eq([:rk_model, :series]) }
      end

      context 'when a selected column is not present as a field, but both in the partition and clustering keys' do
        let(:partition_key) { [:rk_model, :ck_series] }
        let(:select_columns) { [:series] }
        let(:remaining_columns) { [:meta_data] }

        it 'prefers the clustering column over the partition key' do
          is_expected.to eq([:ck_series])
        end
      end
    end

    describe '.normalized_column' do
      let(:key) { 'model' }

      subject { MockRecordStatic.normalized_column(key) }

      it { is_expected.to eq(:model) }

      context 'when the key is part of the row/partition key' do
        let(:key) { 'rk_model' }

        it { is_expected.to eq(:model) }
      end

      context 'when the key is part of the clustering columns' do
        let(:key) { 'ck_model' }

        it { is_expected.to eq(:model) }
      end
    end

    describe '.normalized_attributes' do
      let(:key) { 'model' }
      let(:value) { Faker::Lorem.word }
      let(:attributes) { {key => value} }

      subject { MockRecordStatic.normalized_attributes(attributes) }

      it { is_expected.to eq(model: value) }

      context 'when the key is part of the row/partition key' do
        let(:key) { 'rk_model' }

        it { is_expected.to eq(model: value) }
      end

      context 'when the key is part of the clustering columns' do
        let(:key) { 'ck_model' }

        it { is_expected.to eq(model: value) }
      end
    end

    describe '.select_column' do
      let(:partition_key) { [:rk_model] }
      let(:clustering_columns) { [:ck_model] }
      let(:remaining_columns) { [:model] }
      let(:select_column) { :model }

      subject { MockRecordStatic.select_column(select_column) }

      it { is_expected.to eq(select_column) }

      context 'when the selected column is not present in the field' do
        let(:remaining_columns) { [] }

        context 'when a selected column is only present as a clustering key part' do
          let(:partition_key) { [:rk_series] }

          it { is_expected.to eq(:ck_model) }
        end

        context 'when a selected column is only present as a partition key part' do
          let(:clustering_columns) { [:ck_series] }

          it { is_expected.to eq(:rk_model) }
        end

        context 'when a selected column is not present as a field, but both in the partition and clustering keys' do
          let(:partition_key) { [:rk_model] }

          it 'prefers the clustering column over the partition key' do
            is_expected.to eq(:ck_model)
          end
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

      context 'when restricting the clustering column by a non-equal restriction' do
        let(:query) { 'SELECT * FROM mock_records WHERE rk_model = ? AND rk_series = ? AND ck_price > ?' }

        it 'should map the clustering column properly' do
          expect(connection).to receive(:execute_async).with(statement, 'AABBCCDD', '91A', 9.99, {})
          MockRecordStatic.request_async(model: 'AABBCCDD', series: '91A', :price.gt => 9.99)
        end

        context 'when the KeyComparer represents an array of columns' do
          let(:clustering_columns) { [:ck_price, :ck_version] }
          let(:query) { 'SELECT * FROM mock_records WHERE rk_model = ? AND rk_series = ? AND (ck_price,ck_version) > (?, ?)' }

          it 'should map the clustering column properly' do
            expect(connection).to receive(:execute_async).with(statement, 'AABBCCDD', '91A', 9.99, '003', {})
            MockRecordStatic.request_async(model: 'AABBCCDD', series: '91A', [:price, :version].gt => [9.99, '003'])
          end
        end

        context 'when the clustering column is part of the partition key' do
          let(:defaults) { [{model: ''}, {series: ''}] }
          let(:truth_table) { [[:model], [:series]] }
          let(:partition_key) { [:rk_model, :rk_series] }
          let(:clustering_columns) { [:ck_series] }
          let(:query) { 'SELECT * FROM mock_records WHERE rk_model = ? AND ck_series > ? AND rk_series = ?' }

          it 'should not compare on the partition key' do
            expect(connection).to receive(:execute_async).with(statement, 'AABBCCDD', '91A', '', {})
            MockRecordStatic.request_async(model: 'AABBCCDD', :series.gt => '91A')
          end
        end
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

      context 'with a column ordering specified' do
        let(:query) { 'SELECT * FROM mock_records WHERE rk_model = ? AND rk_series = ? AND ck_price = ? ORDER BY ck_model' }

        it 'should map the composite column to the clustering column' do
          expect(connection).to receive(:execute_async).with(statement, 'AABBCCDD', '91A', 9.99, {})
          MockRecordStatic.request_async({model: 'AABBCCDD', series: '91A', price: 9.99}, order_by: [:model])
        end

        context 'with a specific ordering direction' do
          let(:query) { 'SELECT * FROM mock_records WHERE rk_model = ? AND rk_series = ? AND ck_price = ? ORDER BY ck_model DESC' }

          it 'should map the composite column to the clustering column' do
            expect(connection).to receive(:execute_async).with(statement, 'AABBCCDD', '91A', 9.99, {})
            MockRecordStatic.request_async({model: 'AABBCCDD', series: '91A', price: 9.99}, order_by: [{model: :desc}])
          end
        end

        context 'with the column is not specified in an array' do
          it 'should handle this properly' do
            expect(connection).to receive(:execute_async).with(statement, 'AABBCCDD', '91A', 9.99, {})
            MockRecordStatic.request_async({model: 'AABBCCDD', series: '91A', price: 9.99}, order_by: :model)
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

    describe 'sharding' do
      let(:sharding_column) { :shard }
      let(:klass) { MockRecordStatic }

      it_behaves_like 'a sharding model'

      context 'with a composite shard' do
        let(:sharding_column) { :rk_shard }

        it_behaves_like 'a sharding model'
      end
    end

  end
end
