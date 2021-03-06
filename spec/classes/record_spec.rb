require 'spec_helper'

module CassandraModel
  describe Record do
    let(:table_name) { :records }
    let(:query) { nil }
    let(:partition_key) { [:partition] }
    let(:clustering_columns) { [:cluster] }
    let(:primary_key) { partition_key + clustering_columns }
    let(:remaining_columns) { [] }
    let(:columns) { primary_key + remaining_columns }
    let(:base_record_klass) { NamedClass.create('CassandraModel::Record', Record) {} }
    let(:image_data_klass) { NamedClass.create('CassandraModel::ImageData', base_record_klass) {} }
    let(:klass) { base_record_klass }
    let!(:statement) { mock_prepare(query) }

    before do
      allow(Concurrent::Future).to receive(:execute) do |&block|
        result = block.call
        double(:future, value: result, add_observer: nil)
      end
      mock_simple_table(table_name, partition_key, clustering_columns, columns)
      mock_simple_table(:image_data, partition_key, clustering_columns, columns)
      allow(Logging.logger).to receive(:error)
    end

    it { is_expected.to be_a_kind_of(RecordDebug) }

    describe 'the class' do
      subject { klass }
      it { is_expected.to be_a_kind_of(Scopes) }
    end

    context 'when mixing in query methods' do
      let(:base_results) { MockPage.new(true, nil, ['OK']) }
      let(:base_future) { Cassandra::Future.value(base_results) }

      subject { klass }

      before do
        klass.deferred_column :fake_column, on_load: ->(attributes) {}, on_save: ->(attributes, value) {}
        klass.async_deferred_column :async_fake_column, on_load: ->(attributes) {}, on_save: ->(attributes, value) { Cassandra::Future.value(nil) }
        allow(connection).to receive(:execute_async).and_return(base_future)
      end

      it_behaves_like 'a query helper'
      it_behaves_like 'a record defining meta columns'
    end

    shared_examples_for 'a set of columns' do |method|
      let(:columns) { [:column1] }

      subject { klass }

      before { allow_any_instance_of(TableRedux).to receive(method).and_return(columns) }

      it 'should delegate the method to the underlying table' do
        expect(subject.public_send(method)).to eq(columns)
      end

      context 'with a different result' do
        let(:columns) { [:column1, :column2, :column3] }

        its(method) { is_expected.to eq(columns) }
      end
    end

    describe('.partition_key') { it_behaves_like 'a set of columns', :partition_key }
    describe('.clustering_columns') { it_behaves_like 'a set of columns', :clustering_columns }

    describe '.columns' do
      it_behaves_like 'a set of columns', :columns

      describe 'defining methods for record columns' do

        it 'should define a method to assign and retrieve the column' do
          record = klass.new(partition: 'Partition Key')
          record.partition = 'Different Key'
          expect(record.partition).to eq('Different Key')
        end

        context 'with multiple columns' do
          let(:clustering_columns) { [:clustering] }

          it 'should define a method to assign and retrieve the additional column' do
            record = klass.new(clustering: 'Clustering Key')
            record.clustering = 'Different Key'
            expect(record.clustering).to eq('Different Key')
          end
        end
      end

      describe 'record initialization' do
        it 'should be a valid record initially' do
          record = klass.new(partition: 'Partition Key')
          expect(record.valid).to eq(true)
        end

        describe 'inputs' do
          let(:attributes) { {part: 'Partition', ck: 'Clustering'} }
          let!(:record) { klass.new(attributes, validate: false) }

          it 'should not modify the passed in attributes hash' do
            attributes.delete(:part)
            expect(record.attributes).to include(:part)
          end

          it 'should also not modify underlying attributes' do
            attributes[:part][0..-1] = 'noititraP'
            expect(record.attributes[:part]).to eq('Partition')
          end
        end

        it 'should ensure that the getters are defined' do
          record = klass.new({}, validate: false)
          expect { record.partition }.not_to raise_error
        end

        it 'should ensure that the setters are defined' do
          record = klass.new({}, validate: false)
          expect { record.partition = 'bob' }.not_to raise_error
        end

        it 'can initialize without any paramters' do
          expect(klass.new.attributes).to eq({})
        end

        describe 'working with deferred columns' do
          let(:data) { SecureRandom.uuid }
          let(:new_attributes) { {saved_data: data} }

          subject { klass.new(new_attributes) }

          before { klass.deferred_column :saved_data, on_load: ->(_) { data } }

          its(:saved_data) { is_expected.to eq(data) }
          its(:attributes) { is_expected.not_to include(:saved_data) }

          it 'should leave the input attributes alone' do
            subject
            expect(new_attributes).to include(:saved_data)
          end
        end
      end
    end

    describe '.denormalized_column_map' do
      let(:klass) { base_record_klass }
      let(:expected_map) { (klass.columns & input_columns).inject({}) { |memo, column| memo.merge!(column => column) } }
      let(:input_columns) { klass.columns }

      subject { klass.denormalized_column_map(input_columns) }

      it { is_expected.to eq(expected_map) }

      context 'with a different table' do
        let(:klass) { NamedClass.create('CassandraModel::ImageData', base_record_klass) {} }
        it { is_expected.to eq(expected_map) }
      end

      context 'with a different input list' do
        let(:input_columns) { [:partition] }
        it { is_expected.to eq(expected_map) }
      end

      context 'with an input list containing extra columns' do
        let(:input_columns) { [:partition, :some_unk_field] }
        it { is_expected.to eq(expected_map) }
      end
    end

    describe '.composite_defaults' do
      subject { klass.composite_defaults }
      it { is_expected.to eq([]) }
    end

    describe '.table_name' do
      it 'should be the lower-case plural of the class' do
        expect(klass.table_name).to eq('records')
      end

      context 'when inherited from a different class' do
        it { expect(image_data_klass.table_name).to eq('image_data') }
      end

      context 'when overridden' do
        before { klass.table_name = 'image_data' }
        it { expect(klass.table_name).to eq('image_data') }
      end
    end

    describe '.connection_name' do
      it 'should use the default connection' do
        expect(klass.table.connection).to eq(ConnectionCache[nil])
      end

      context 'when overridden' do
        let(:connection_name) { :counters }

        before { klass.connection_name = connection_name }

        it 'should use the specified connection' do
          expect(klass.table.connection).to eq(ConnectionCache[:counters])
        end
      end
    end

    describe '.table=' do
      it 'should allow the user to overwrite the default table behaviour' do
        klass.table = TableRedux.new('week 1 table')
        expect(klass.table_name).to eq('week 1 table')
      end
    end

    describe '.predecessor' do
      it 'should be nil' do
        expect(klass.predecessor).to be_nil
      end

      context 'when overridden' do
        let(:predecessor) { image_data_klass }

        before { klass.predecessor = predecessor }

        it 'should use the specified connection' do
          expect(klass.predecessor).to eq(image_data_klass)
        end
      end
    end

    describe '.query_for_save' do
      let(:columns) { [:partition] }
      let(:clustering_columns) { [] }

      it 'should represent the query for saving all the column values' do
        expect(klass.query_for_save).to eq('INSERT INTO records (partition) VALUES (?)')
      end

      context 'with different columns defining the row key' do
        let(:clustering_columns) { [:cluster] }

        it 'should represent the query for saving all the column values' do
          expect(klass.query_for_save).to eq('INSERT INTO records (partition, cluster) VALUES (?, ?)')
        end
      end

      context 'with a different record type/table name' do
        let(:klass) { image_data_klass }

        it 'should represent the query for saving all the column values' do
          expect(klass.query_for_save).to eq('INSERT INTO image_data (partition) VALUES (?)')
        end
      end
    end

    describe '.query_for_delete' do
      let(:partition_key) { [:partition] }
      let(:clustering_columns) { [] }

      it 'should represent the query for saving all the column values' do
        expect(klass.query_for_delete).to eq('DELETE FROM records WHERE partition = ?')
      end

      context 'with different columns' do
        let(:clustering_columns) { [:cluster] }

        it 'should represent the query for deleting all the column values' do
          expect(klass.query_for_delete).to eq('DELETE FROM records WHERE partition = ? AND cluster = ?')
        end
      end

      context 'with a different record type/table name' do
        let(:klass) { image_data_klass }

        it 'should represent the query for saving all the column values' do
          expect(klass.query_for_delete).to eq('DELETE FROM image_data WHERE partition = ?')
        end
      end
    end

    describe '.create_async' do
      let(:attributes) { {partition: 'Partition Key'} }
      let(:record) { klass.new(attributes) }
      let(:future_record) { Cassandra::Future.value(record) }
      let(:options) { {} }

      before do
        allow_any_instance_of(klass).to receive(:save_async).with(options).and_return(future_record)
      end

      it 'should return a new record instance with the specified attributes' do
        expect(klass.create_async(attributes).get).to eq(record)
      end

      context 'when options are provided' do
        let(:options) { {check_exists: true} }

        it 'should return a new record instance with the specified attributes' do
          expect(klass.create_async(attributes, options).get).to eq(record)
        end
      end

      context 'with a different record type' do
        let(:klass) { image_data_klass }

        it 'should create an instance of that record' do
          expect(image_data_klass).to receive(:new).with(attributes).and_return(record)
          image_data_klass.create_async(attributes)
        end
      end
    end

    describe '.normalized_column' do
      let(:key) { Faker::Lorem.word }

      subject { klass.normalized_column(key) }

      it { is_expected.to eq(key.to_sym) }
    end

    describe '.restriction_attributes' do
      let(:restriction) { {city: Faker::Address.city, street: Faker::Address.street_name} }

      subject { klass.restriction_attributes(restriction) }

      it { is_expected.to eq(restriction) }
    end

    describe '.normalized_attributes' do
      let(:key) { Faker::Lorem.word }
      let(:attributes) { {key => Faker::Lorem.word} }

      subject { klass.normalized_attributes(attributes) }

      it { is_expected.to eq(attributes.symbolize_keys) }
    end

    describe '.select_columns' do
      let(:columns) { Faker::Lorem.words.map(&:to_sym) }

      subject { klass.select_columns(columns) }

      it { is_expected.to eq(columns) }
    end

    describe '.select_column' do
      let(:column) { Faker::Lorem.word }

      subject { klass.select_column(column) }

      it { is_expected.to eq(column) }
    end

    describe '.cassandra_columns' do
      let(:cassandra_columns) do
        3.times.map do
          Cassandra::Column.new(Faker::Lorem.word, %w(int text timestamp).sample.to_sym, nil)
        end
      end
      let(:column_type_map) do
        cassandra_columns.inject({}) do |memo, column|
          memo.merge!(column.name.to_sym => column.type)
        end
      end
      let(:cassandra_table) { double(:table, columns: cassandra_columns) }

      subject { klass.cassandra_columns }

      before do
        allow(klass.table.connection.keyspace).to receive(:table).and_return(cassandra_table)
      end

      it { is_expected.to eq(column_type_map) }
    end

    describe '.request_async' do
      let(:clause) { {} }
      let(:where_clause) { nil }
      let(:limit_clause) { nil }
      let(:table_name) { :table }
      let(:select_clause) { '*' }
      let(:order_clause) { nil }
      let(:query) { "SELECT #{select_clause} FROM #{table_name}#{where_clause}#{order_clause}#{limit_clause}" }
      let(:page_results) { ['partition' => 'Partition Key'] }
      let(:result_page) { MockPage.new(true, Cassandra::Future.value([]), page_results) }
      let(:results) { Cassandra::Future.value(result_page) }
      let(:execution_info) { result_page.execution_info }
      let(:record) { klass.new(partition: 'Partition Key') }
      let(:remaining_columns) { [:time_stamp] }
      let(:duration) { rand }

      before do
        klass.table_name = table_name
        allow(connection).to receive(:execute_async).with(statement, *clause.values, {}).and_return(results)
      end

      it 'should create a Record instance for each returned result' do
        expect(klass.request_async(clause).get.first).to eq(record)
      end

      it 'should log the time it took the request to complete' do
        allow_any_instance_of(ThomasUtils::Observation).to receive(:on_timed).and_yield(nil, nil, duration, page_results, nil)
        expect(Logging.logger).to receive(:debug) do |&block|
          expect(block.call).to eq("#{klass} Load (Page 1 with count 1): #{duration * 1000}ms")
        end
        klass.request_async(clause).get
      end

      describe 'saving the execution info for a single result' do
        let(:limit_clause) { ' LIMIT 1' }

        it 'should return a ThomasUtils::Observation' do
          expect(klass.request_async(clause, limit: 1)).to be_a_kind_of(ThomasUtils::Observation)
        end

        it 'should log the time it took the request to complete' do
          allow_any_instance_of(ThomasUtils::Observation).to receive(:on_timed).and_yield(nil, nil, duration, nil, nil)
          expect(Logging.logger).to receive(:debug) do |&block|
            expect(block.call).to eq("#{klass} Load: #{duration * 1000}ms")
          end
          klass.request_async(clause, limit: 1)
        end

        it 'should save the execution info from the query result when querying for one record' do
          expect(klass.request_async(clause, limit: 1).get.execution_info).to eq(execution_info)
        end
      end

      it 'should save the execution info from the query result when querying for multiple record' do
        expect(klass.request_async(clause).get.first.execution_info).to eq(execution_info)
      end

      context 'when the restriction key is a KeyComparer' do
        let(:clause) { {:partition.gt => 'Partition Key'} }
        let(:where_clause) { ' WHERE partition > ?' }

        it 'should query using the specified comparer' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', {}).and_return(results)
          klass.request_async(clause)
        end

        context 'when the KeyComparer maps to an array' do
          let(:clustering_columns) { [:price, :model] }
          let(:clause) { {[:price, :model].gt => [999.98, 'ATF50']} }
          let(:where_clause) { ' WHERE (price,model) > (?, ?)' }

          it 'should query using all params' do
            expect(connection).to receive(:execute_async).with(statement, 999.98, 'ATF50', {}).and_return(results)
            klass.request_async(clause)
          end
        end
      end

      context 'with a read consistency configured' do
        let(:consistency) { :quorum }

        before { klass.read_consistency = consistency }

        it 'should query using the specified consistency' do
          expect(connection).to receive(:execute_async).with(statement, consistency: consistency).and_return(results)
          klass.request_async(clause)
        end

        context 'with a different consistency' do
          let(:consistency) { :all }

          it 'should query using the specified consistency' do
            expect(connection).to receive(:execute_async).with(statement, consistency: consistency).and_return(results)
            klass.request_async(clause)
          end
        end
      end

      context 'when tracing is specified' do
        it 'should forward tracing to the underlying query execution' do
          expect(connection).to receive(:execute_async).with(statement, trace: true).and_return(results)
          klass.request_async(clause, trace: true)
        end
      end

      context 'when restricting by multiple values' do
        let(:clause) { {partition: ['Partition Key', 'Other Partition Key']} }
        let(:where_clause) { ' WHERE partition IN (?, ?)' }
        let(:results) { Cassandra::Future.value([{'partition' => 'Partition Key'}, {'partition' => 'Other Partition Key'}]) }

        it 'should query using an IN' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Other Partition Key', {}).and_return(results)
          klass.request_async(clause)
        end
      end

      context 'when selecting a subset of columns' do
        let(:options) { {select: :partition} }
        let(:select_clause) { :partition }
        let(:record) { klass.new(partition: 'Partition Key') }

        it 'should return a new instance of the klass with only that attribute assigned' do
          expect(klass.request_async({}, options).get.first).to eq(record)
        end

        it 'should invalidate the record' do
          expect(klass.request_async({}, options).get.first.valid).to eq(false)
        end

        context 'with multiple columns selected' do
          let(:options) { {select: [:partition, :cluster]} }
          let(:select_clause) { %w(partition cluster).join(', ') }
          let(:page_results) { [{'partition' => 'Partition Key', cluster: 'Cluster Key'}] }
          let(:record) { klass.new(partition: 'Partition Key', cluster: 'Cluster Key') }

          it 'should select all the specified columns' do
            expect(klass.request_async({}, options).get.first).to eq(record)
          end
        end
      end

      context 'when ordering by a subset of columns' do
        let(:options) { {order_by: :cluster} }
        let(:order_clause) { ' ORDER BY cluster' }
        let(:page_results) do
          [
              {'partition' => 'Partition Key', cluster: 'Cluster Key', other_cluster: 'Other Cluster Key'},
              {'partition' => 'Partition Key', cluster: 'Cluster Key 2', other_cluster: 'Other Cluster Key'},
              {'partition' => 'Partition Key', cluster: 'Cluster Key 2', other_cluster: 'Other Cluster Key 2'}
          ]
        end
        let(:record_one) { klass.new(partition: 'Partition Key', cluster: 'Cluster Key', other_cluster: 'Other Cluster Key') }
        let(:record_two) { klass.new(partition: 'Partition Key', cluster: 'Cluster Key 2', other_cluster: 'Other Cluster Key') }
        let(:record_three) { klass.new(partition: 'Partition Key', cluster: 'Cluster Key 2', other_cluster: 'Other Cluster Key 2') }
        let(:clustering_columns) { [:cluster, :other_cluster] }
        let(:remaining_columns) { [] }

        it 'should order the results by the specified column' do
          expect(klass.request_async({}, options).get).to eq([record_one, record_two, record_three])
        end

        context 'with a direction specified' do
          let(:direction) { :desc }
          let(:options) { {order_by: [{cluster: direction}]} }
          let(:order_clause) { ' ORDER BY cluster DESC' }

          it 'should order the results  the specified direction' do
            expect(klass.request_async({}, options).get).to eq([record_one, record_two, record_three])
          end

          context 'with a different direction' do
            let(:direction) { :asc }
            let(:order_clause) { ' ORDER BY cluster ASC' }

            it 'should order the results  the specified direction' do
              expect(klass.request_async({}, options).get).to eq([record_one, record_two, record_three])
            end
          end
        end

        context 'with multiple columns selected' do
          let(:options) { {order_by: [:cluster, :other_cluster]} }
          let(:order_clause) { ' ORDER BY cluster, other_cluster' }

          it 'should order by all the specified columns' do
            expect(klass.request_async({}, options).get).to eq([record_one, record_two, record_three])
          end
        end
      end

      context 'with a different record type' do
        let(:table_name) { :image_data }

        it 'should return records of that type' do
          expect(image_data_klass.request_async(clause).get.first).to be_a_kind_of(image_data_klass)
        end
      end

      context 'with multiple results' do
        let(:options) { {limit: 2} }
        let(:where_clause) { ' LIMIT 2' }
        let(:results) { Cassandra::Future.value([{'partition' => 'Partition Key 1'}, {'partition' => 'Partition Key 2'}]) }

        it 'should support limits' do
          expect(connection).to receive(:execute_async).with(statement, {}).and_return(results)
          klass.request_async({}, options)
        end

        context 'with a strange limit' do
          let(:options) { {limit: 'bob'} }

          it 'should raise an error' do
            expect { klass.request_async({}, options) }.to raise_error("Invalid limit 'bob'")
          end
        end
      end

      context 'with no clause' do
        it 'should query for everything' do
          expect(connection).to receive(:execute_async).with(statement, {}).and_return(results)
          klass.request_async(clause)
        end
      end

      context 'using only the partition key' do
        let(:clause) do
          {
              partition: 'Partition Key'
          }
        end
        let(:where_clause) { ' WHERE partition = ?' }

        it 'should return the result of a select query given a restriction' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', {}).and_return(results)
          klass.request_async(clause)
        end
      end

      context 'using a clustering key' do
        let(:clause) do
          {
              partition: 'Partition Key',
              cluster: 'Cluster Key'
          }
        end
        let(:where_clause) { ' WHERE partition = ? AND cluster = ?' }

        it 'should return the result of a select query given a restriction' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Cluster Key', {}).and_return(results)
          klass.request_async(clause)
        end
      end

      context 'when paginating over results' do
        let(:options) { {page_size: 2} }
        let(:first_page_results) { [{'partition' => 'Partition Key 1'}, {'partition' => 'Partition Key 2'}] }
        let(:first_page) { MockPage.new(true, nil, first_page_results) }
        let(:first_page_future) { Cassandra::Future.value(first_page) }

        it 'should return an enumerable capable of producing all the records' do
          allow(connection).to receive(:execute_async).with(statement, page_size: 2).and_return(first_page_future)
          results = []
          klass.request_async({}, options).each do |result|
            results << result
          end
          expected_records = [
              klass.new(partition: 'Partition Key 1'),
              klass.new(partition: 'Partition Key 2')
          ]
          expect(results).to eq(expected_records)
        end
      end

      context 'when using options and restrictions' do
        let(:clause) { {partition: 'Partition Key', cluster: 'Cluster Key'} }
        let(:options) { {select: [:partition, :cluster], order_by: :cluster, limit: 100} }
        let(:where_clause) { ' WHERE partition = ? AND cluster = ? ORDER BY cluster LIMIT 100' }
        let(:select_clause) { 'partition, cluster' }

        it 'should order options and restrictions in the query properly' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Cluster Key', {}).and_return(results)
          klass.request_async(clause, options)
        end
      end
    end

    describe '.first_async' do
      let(:request_attributes) { ['Partition Key'] }
      let(:clause) { {partition: 'Partition Key'} }
      let(:options) { {select: :partition} }
      let(:record) { klass.new(partition: 'Partition Key') }

      let(:query) { 'SELECT partition FROM records WHERE partition = ? LIMIT 1' }
      let(:page_results) { ['partition' => 'Partition Key'] }
      let(:result_page) { MockPage.new(true, Cassandra::Future.value([]), page_results) }
      let(:results) { Cassandra::Future.value(result_page) }

      before do
        klass.table_name = table_name
        allow(connection).to receive(:execute_async).with(statement, *request_attributes, {}).and_return(results)
      end

      it 'should delegate to request using a limit of 1' do
        expect(klass.first_async(clause, options).get).to eq(record)
      end

      context 'when the request returns no results' do
        let(:page_results) { [] }

        it 'should return nil' do
          expect(klass.first_async(clause, options).get).to be_nil
        end
      end

      context 'when the request clause is omitted' do
        let(:request_attributes) { [] }
        let(:query) { 'SELECT * FROM records LIMIT 1' }

        it 'should default the request clause to {}' do
          expect(klass.first_async.get).to eq(record)
        end
      end
    end

    shared_examples_for 'a method creating a record' do |method|
      let(:attributes) { {partition: 'Partition Key'} }
      let(:record) { klass.new(attributes) }
      let(:future_record) { Cassandra::Future.value(record) }
      let(:options) { {} }

      before do
        allow(klass).to receive(:create_async).with(attributes, options).and_return(future_record)
      end

      it 'should resolve the future returned by .create_async' do
        expect(klass.public_send(method, attributes)).to eq(record)
      end

      context 'when options are provided' do
        let(:options) { {check_exists: true} }

        it 'should resolve the future returned by .create_async' do
          expect(klass.public_send(method, attributes, options)).to eq(record)
        end
      end
    end

    describe('.create') { it_behaves_like 'a method creating a record', :create }
    describe('.create!') { it_behaves_like 'a method creating a record', :create! }

    describe '.request' do
      let(:clause) { {} }
      let(:options) { {limit: 1} }
      let(:record) { klass.new(partition: 'Partition Key') }
      let(:future_record) { Cassandra::Future.value([record]) }

      it 'should resolve the future provided by request_async' do
        allow(klass).to receive(:request_async).with(clause, options).and_return(future_record)
        expect(klass.request(clause, options)).to eq([record])
      end
    end

    describe '.first' do
      let(:clause) { {} }
      let(:options) { {select: :partition} }
      let(:record) { double(:record) }
      let(:future_record) { Cassandra::Future.value(record) }

      it 'should resolve the future provided by first_async' do
        allow(klass).to receive(:first_async).with(clause, options).and_return(future_record)
        expect(klass.first(clause, options)).to eq(record)
      end

      it 'should default the request clause to {}' do
        expect(klass).to receive(:first_async).with({}, {}).and_return(future_record)
        klass.first
      end
    end

    describe 'sharding' do
      let(:sharding_column) { :shard }

      it_behaves_like 'a sharding model'
    end

    describe '#attributes' do
      it 'should return the attributes of the created Record' do
        record = klass.new(partition: 'Partition Key')
        expect(record.attributes).to eq(partition: 'Partition Key')
      end

      context 'with an invalid column' do
        it 'should raise an error' do
          expect { klass.new(fake_column: 'Partition Key') }.to raise_error("Invalid column 'fake_column' specified")
        end

        context 'when validation is disabled' do
          it 'should not raise an error' do
            expect { klass.new({fake_column: 'Partition Key'}, validate: false) }.not_to raise_error
          end
        end
      end
    end

    describe 'columns defining a primary key' do
      let(:partition_key) { [:pk1] }
      let(:clustering_columns) { [:ck1] }
      let(:remaining_columns) { [:field] }
      let(:attributes) { {pk1: 'Some pk', ck1: 'Some ck', field: 'data'} }
      let(:record) { klass.new(attributes) }

      describe '#partition_key' do

        it 'should return the slice of attributes representing the partition key' do
          expect(record.partition_key).to eq(pk1: 'Some pk')
        end

        context 'with a different table' do
          let(:partition_key) { [:part1, :part2] }
          let(:attributes) { {part1: 'Bag', part2: 'Large', ck1: 'Some ck', field: 'data'} }

          it 'should return the slice of attributes representing the partition key' do
            expect(record.partition_key).to eq(part1: 'Bag', part2: 'Large')
          end
        end
      end

      describe '#clustering_columns' do
        it 'should return the slice of attributes representing the clustering columns' do
          expect(record.clustering_columns).to eq(ck1: 'Some ck')
        end

        context 'with a different table' do
          let(:clustering_columns) { [:clust1, :clust2] }
          let(:attributes) { {pk1: 'Bag', clust1: 'Brick', clust2: 'Wall', field: 'data'} }

          it 'should return the slice of attributes representing the clustering columns' do
            expect(record.clustering_columns).to eq(clust1: 'Brick', clust2: 'Wall')
          end
        end
      end

      describe '#primary_key' do
        it 'should return the slice of attributes representing the primary key' do
          expect(record.primary_key).to eq(pk1: 'Some pk', ck1: 'Some ck')
        end

        context 'with a different table' do
          let(:partition_key) { [:part1, :part2] }
          let(:clustering_columns) { [:clust1, :clust2] }
          let(:attributes) { {part1: 'Box', part2: 'Big', clust1: 'Brick', clust2: 'Wall', field: 'data'} }

          it 'should return the slice of attributes representing the clustering columns' do
            expect(record.primary_key).to eq(part1: 'Box', part2: 'Big', clust1: 'Brick', clust2: 'Wall')
          end
        end
      end
    end

    describe '#save_async' do
      let(:table_name) { :table }
      let(:clustering_columns) { [] }
      let(:attributes) { {partition: 'Partition Key'} }
      let(:existence_check) { nil }
      let(:ttl_clause) { nil }
      let(:query) { "INSERT INTO table (#{columns.join(', ')}) VALUES (#{(%w(?) * columns.size).join(', ')})#{existence_check}#{ttl_clause}" }
      let(:query_results) { [] }
      let(:page_results) { MockPage.new(true, nil, query_results) }
      let(:execution_info) { page_results.execution_info }
      let(:future_result) { page_results }
      let(:future_error) { nil }
      let(:results) do
        future_error ? Cassandra::Future.error(future_error) : Cassandra::Future.value(page_results)
      end

      before do
        klass.table_name = table_name
        allow(connection).to receive(:execute_async).and_return(results)
      end

      it 'should return a ThomasUtils::Observation' do
        expect(klass.new(attributes).save_async).to be_a_kind_of(ThomasUtils::Observation)
      end

      it 'should save the record to the database' do
        expect(connection).to receive(:execute_async).with(statement, 'Partition Key', {}).and_return(results)
        klass.new(attributes).save_async
      end

      it 'should call the associated global callback' do
        record = klass.new(attributes)
        expect(GlobalCallbacks).to receive(:call).with(:record_saved, record)
        record.save_async
      end

      context 'with tracing specified' do
        it 'should execute the query with tracing enabled' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', trace: true).and_return(results)
          klass.new(attributes).save_async(trace: true)
        end
      end

      it 'should assign the execution_info for this record' do
        record = klass.new(attributes)
        record.save_async
        expect(record.execution_info).to eq(execution_info)
      end

      context 'when the Record class has deferred columns' do
        let(:record) { klass.new(attributes) }
        let(:save_block) { ->(attributes, value) {} }

        before do
          allow(ThomasUtils::Future).to(receive(:new)) do |&block|
            ThomasUtils::Future.immediate(&block)
          end
          klass.deferred_column :fake_column, on_load: ->(attributes) {}, on_save: save_block
          klass.async_deferred_column :async_fake_column, on_load: ->(attributes) {}, on_save: save_block
        end

        it 'should return a ThomasUtils::Observation' do
          expect(klass.new(attributes).save_async).to be_a_kind_of(ThomasUtils::Observation)
        end

        it 'should wrap everything in a future' do
          expect(ThomasUtils::Future).to receive(:new) do |&block|
            expect(klass).to receive(:save_deferred_columns).with(record).and_return([])
            expect(klass).to receive(:save_async_deferred_columns).with(record).and_return([])
            ThomasUtils::Future.immediate(&block).then { Cassandra::Future.value(record) }
          end
          record.save_async
        end

        describe 'saving the execution info' do
          before do
            allow(klass).to receive(:save_deferred_columns).with(record).and_return([])
            allow(klass).to receive(:save_async_deferred_columns).with(record).and_return([])
          end

          it 'should assign the execution_info for this record' do
            record = klass.new(attributes)
            record.save_async
            expect(record.execution_info).to eq(execution_info)
          end

          it 'should call the associated global callback' do
            record = klass.new(attributes)
            expect(GlobalCallbacks).to receive(:call).with(:record_saved, record)
            record.save_async
          end
        end

        context 'when the block raises an error' do
          let(:error) { Exception }
          let(:error_message) { 'Death #' + SecureRandom.uuid }
          let(:save_block) { ->(_, _) { raise error, error_message } }

          it 'should resolve to a future raising that error' do
            expect { record.save_async.get }.to raise_error(error, error_message)
          end
        end

        context 'when specifying explicitly not to save deferred columns' do
          it 'should not save them' do
            expect(klass).not_to receive(:save_deferred_columns)
            expect(klass).not_to receive(:save_async_deferred_columns)
            record.save_async(skip_deferred_columns: true)
          end
        end
      end

      context 'when the record has been invalidated' do
        before { allow_any_instance_of(klass).to receive(:valid).and_return(false) }

        it 'should raise an error' do
          expect { klass.new(attributes).save_async }.to raise_error('Cannot save invalidated record!')
        end
      end

      context 'when configured to use a batch' do
        subject { klass }
        it_behaves_like 'a query running in a batch', :save_async, [], ['Partition Key']
      end

      context 'when a consistency is specified' do
        let(:consistency) { :quorum }

        before { klass.write_consistency = consistency }

        it 'should save the record to the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', consistency: consistency).and_return(results)
          klass.new(attributes).save_async
        end

        context 'with a different consistency' do
          let(:consistency) { :all }

          it 'should save the record to the database' do
            expect(connection).to receive(:execute_async).with(statement, 'Partition Key', consistency: consistency).and_return(results)
            klass.new(attributes).save_async
          end
        end
      end

      it 'should not log an error' do
        expect(Logging.logger).not_to receive(:error)
        klass.new(attributes).save_async
      end

      context 'when part of the primary key is missing' do
        let(:partition_key) { [:part1, :part2] }
        let(:clustering_columns) { [:ck1, :ck2] }
        let(:remaining_columns) { [] }
        let(:attributes) { {part1: 'Part 1', ck2: 'Does not matter'} }
        let(:record_instance) { klass.new(attributes) }
        let(:column_values) { (partition_key + clustering_columns + remaining_columns).map { |key| attributes[key] } }
        let(:record_saved_future) { record_instance.save_async }
        let(:error_message) { 'Invalid primary key parts "part2", "ck1"' }

        subject { record_saved_future.get }

        it 'should raise an Cassandra::Invalid error' do
          expect { subject }.to raise_error(Cassandra::Errors::InvalidError, error_message)
        end

        it 'should log the error' do
          expect(Logging.logger).to receive(:error).with("Error saving CassandraModel::Record: #{error_message}")
          subject rescue nil
        end

        it 'should call the associated global callback' do
          expect(GlobalCallbacks).to receive(:call).with(:save_record_failed, record_instance, a_kind_of(Cassandra::Errors::InvalidError), statement, column_values)
          subject rescue nil
        end

        context 'when there is only one partition key part and it is an empty string' do
          let(:partition_key) { [:part1] }
          let(:attributes) { {part1: '', ck1: 'Also does not matter', ck2: 'Does not matter'} }
          let(:error_message) { 'Invalid primary key parts "part1"' }

          it 'should raise an Cassandra::Invalid error' do
            expect { subject }.to raise_error(Cassandra::Errors::InvalidError, error_message)
          end
        end
      end

      context 'when an error occurs' do
        let(:future_error) { StandardError.new('IOError: Connection Closed') }
        let(:record_instance) { klass.new(attributes) }
        let(:column_values) { record_instance.attributes.values }

        it 'should log the error' do
          expect(Logging.logger).to receive(:error).with('Error saving CassandraModel::Record: IOError: Connection Closed')
          record_instance.save_async
        end

        it 'should execute the save record failed callback' do
          expect(GlobalCallbacks).to receive(:call).with(:save_record_failed, record_instance, future_error, statement, column_values)
          record_instance.save_async
        end

        context 'with a different error' do
          let(:future_error) { StandardError.new('Error, received only 2 responses') }

          it 'should log the error' do
            expect(Logging.logger).to receive(:error).with('Error saving CassandraModel::Record: Error, received only 2 responses')
            record_instance.save_async
          end
        end

        context 'with a different model' do
          let(:record_instance) { image_data_klass.new(attributes) }

          it 'should log the error' do
            expect(Logging.logger).to receive(:error).with('Error saving CassandraModel::ImageData: IOError: Connection Closed')
            record_instance.save_async
          end
        end
      end

      it 'should return a future resolving to the record instance' do
        record = klass.new(partition: 'Partition Key')
        expect(record.save_async.get).to eq(record)
      end

      describe 'setting a TTL (time-to-live) to a record' do
        let(:ttl) { rand(1..999) }
        let(:ttl_clause) { " USING TTL #{ttl}" }

        it 'should save the record to the database using the specified TTL' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', {}).and_return(results)
          klass.new(attributes).save_async(ttl: ttl)
        end
      end

      describe 'checking if the record already exists' do
        let(:existence_check) { ' IF NOT EXISTS' }

        it 'should save the record to the database, checking if it had previously existed' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', {}).and_return(results)
          klass.new(attributes).save_async(check_exists: true)
        end

        context 'when a consistency is specified' do
          let(:consistency) { :local_serial }

          before { klass.serial_consistency = consistency }

          it 'should save the record to the database' do
            expect(connection).to receive(:execute_async).with(statement, 'Partition Key', serial_consistency: consistency).and_return(results)
            klass.new(attributes).save_async(check_exists: true)
          end

          context 'with a different consistency' do
            let(:consistency) { :serial }

            it 'should save the record to the database' do
              expect(connection).to receive(:execute_async).with(statement, 'Partition Key', serial_consistency: consistency).and_return(results)
              klass.new(attributes).save_async(check_exists: true)
            end
          end
        end

        it 'should NOT invalidate the record if it does not yet exist' do
          expect(klass.new(attributes).save_async(check_exists: true).get.valid).to eq(true)
        end

        context 'when the record already exists' do
          let(:query_results) { [{'[applied]' => false}] }

          it 'should invalidate the record if it already exists' do
            expect(klass.new(attributes).save_async(check_exists: true).get.valid).to eq(false)
          end
        end
      end

      context 'with different columns' do
        let(:clustering_columns) { [:cluster] }
        let(:attributes) { {partition: 'Partition Key', cluster: 'Cluster Key'} }

        it 'should save the record to the database using the specified attributes' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Cluster Key', {}).and_return(results)
          klass.new(attributes).save_async
        end
      end
    end

    shared_examples_for 'a method saving a record' do |method|
      let(:attributes) { {partition: 'Partition Key'} }
      let(:record) { klass.new(attributes) }
      let(:record_future) { Cassandra::Future.value(record) }
      let(:options) { {} }

      before { allow(record).to receive(:save_async).with(options).and_return(record_future) }

      it 'should save the record' do
        expect(record).to receive(:save_async).with(options).and_return(record_future)
        record.save
      end

      it 'should resolve the future of #save_async' do
        expect(record.public_send(method)).to eq(record)
      end

      context 'when options are provided' do
        let(:options) { {check_exists: true} }

        it 'should resolve the future of #save_async' do
          expect(record.public_send(method, options)).to eq(record)
        end
      end
    end

    describe('#save') { it_behaves_like 'a method saving a record', :save }
    describe('#save!') { it_behaves_like 'a method saving a record', :save! }

    describe '#invalidate!' do
      it 'should invalidate the Record' do
        record = klass.new({})
        record.invalidate!
        expect(record.valid).to eq(false)
      end
    end

    describe '#delete_async' do
      let(:partition_key) { [:partition] }
      let(:clustering_columns) { [:cluster] }
      let(:attributes) { {partition: 'Partition Key', cluster: 'Cluster Key'} }
      let(:table_name) { :table }
      let(:where_clause) { (partition_key + clustering_columns).map { |column| "#{column} = ?" }.join(' AND ') }
      let(:query) { "DELETE FROM #{table_name} WHERE #{where_clause}" }
      let(:results) { Cassandra::Future.value([]) }

      before do
        klass.table_name = table_name
        allow(klass).to receive(:partition_key).and_return(partition_key)
        allow(klass).to receive(:clustering_columns).and_return(clustering_columns)
        allow(connection).to receive(:execute_async).and_return(results)
      end

      it 'should delete the record from the database' do
        expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Cluster Key', {})
        klass.new(attributes).delete_async
      end

      context 'when configured to use a batch' do
        subject { klass }
        it_behaves_like 'a query running in a batch', :delete_async, [], ['Partition Key', 'Cluster Key']
      end

      context 'when a consistency is specified' do
        let(:consistency) { :quorum }

        before { klass.write_consistency = consistency }

        it 'should delete the record from the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Cluster Key', consistency: consistency)
          klass.new(attributes).delete_async
        end

        context 'with a different consistency' do
          let(:consistency) { :all }

          it 'should delete the record from the database' do
            expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Cluster Key', consistency: consistency)
            klass.new(attributes).delete_async
          end
        end
      end

      it 'should return a future resolving to the record instance' do
        record = klass.new(partition: 'Partition Key')
        expect(record.delete_async.get).to eq(record)
      end

      it 'should invalidate the record instance' do
        record = klass.new(partition: 'Partition Key')
        record.delete_async
        expect(record.valid).to eq(false)
      end

      context 'with different attributes' do
        let(:attributes) { {partition: 'Different Partition Key', cluster: 'Different Cluster Key'} }

        it 'should delete the record from the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Different Partition Key', 'Different Cluster Key', {})
          klass.new(attributes).delete_async
        end
      end

      context 'with a different table name' do
        let(:table_name) { :image_data }

        it 'should delete the record from the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Cluster Key', {})
          klass.new(attributes).delete_async
        end
      end

      context 'with different partition and clustering keys' do
        let(:partition_key) { [:different_partition] }
        let(:clustering_columns) { [:different_cluster] }
        let(:attributes) { {different_partition: 'Partition Key', different_cluster: 'Cluster Key'} }

        it 'should delete the record from the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Cluster Key', {})
          klass.new(attributes).delete_async
        end
      end
    end

    describe '#delete' do
      let(:attributes) { {partition: 'Partition Key'} }
      let(:record) { klass.new(attributes) }
      let(:record_future) { Cassandra::Future.value(record) }

      it 'should delete the record' do
        expect(record).to receive(:delete_async).and_return(record_future)
        record.delete
      end

      it 'should resolve the future of #delete_async' do
        allow(record).to receive(:delete_async).and_return(record_future)
        expect(record.delete).to eq(record)
      end
    end

    describe '#update_async' do
      let(:partition_key) { [:partition] }
      let(:clustering_columns) { [:cluster] }
      let(:remaining_columns) { [:meta_data, :misc_data] }
      let(:columns) { partition_key + clustering_columns + remaining_columns }
      let(:attributes) { {partition: 'Partition Key', cluster: 'Cluster Key'} }
      let(:table_name) { :table }
      let(:where_clause) { (partition_key + clustering_columns).map { |column| "#{column} = ?" }.join(' AND ') }
      let(:new_attributes) { {meta_data: 'Some Data'} }
      let(:query) { "UPDATE #{table_name} SET meta_data = ? WHERE #{where_clause}" }
      let(:results) { Cassandra::Future.value([]) }

      before do
        klass.table_name = table_name
        allow(connection).to receive(:execute_async).and_return(results)
        mock_simple_table(:records, partition_key, clustering_columns, columns)
      end

      it 'should update the record in the database' do
        expect(connection).to receive(:execute_async).with(statement, 'Some Data', 'Partition Key', 'Cluster Key', {})
        klass.new(attributes).update_async(new_attributes)
      end

      context 'when configured to use a batch' do
        subject { klass }
        it_behaves_like 'a query running in a batch', :update_async, [meta_data: 'Some Data'], ['Some Data', 'Partition Key', 'Cluster Key']
      end

      context 'when a consistency is specified' do
        let(:consistency) { :quorum }

        before { klass.write_consistency = consistency }

        it 'should update the record in the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Some Data', 'Partition Key', 'Cluster Key', consistency: consistency)
          klass.new(attributes).update_async(new_attributes)
        end

        context 'with a different consistency' do
          let(:consistency) { :all }

          it 'should update the record in the database' do
            expect(connection).to receive(:execute_async).with(statement, 'Some Data', 'Partition Key', 'Cluster Key', consistency: consistency)
            klass.new(attributes).update_async(new_attributes)
          end
        end
      end

      context 'with an invalid column' do
        let(:new_attributes) { {fake_column: 'Some Fake Data'} }

        it 'should raise an error' do
          expect { klass.new(attributes).update_async(new_attributes) }.to raise_error("Invalid column 'fake_column' specified")
        end
      end

      context 'when updating a single key of a map' do
        let(:new_attributes) { {:meta_data.index('Location') => 'North America'} }
        let(:query) { "UPDATE #{table_name} SET meta_data['Location'] = ? WHERE #{where_clause}" }

        it 'should update only the value of that key for the map' do
          expect(connection).to receive(:execute_async).with(statement, 'North America', 'Partition Key', 'Cluster Key', {})
          klass.new(attributes).update_async(new_attributes)
        end
      end

      context 'with multiple new attributes' do
        let(:new_attributes) { {meta_data: 'meta-data', misc_data: 'Some additional information'} }
        let(:query) { "UPDATE #{table_name} SET meta_data = ?, misc_data = ? WHERE #{where_clause}" }

        it 'should update the record in the database with those attributes' do
          expect(connection).to receive(:execute_async).with(statement, 'meta-data', 'Some additional information', 'Partition Key', 'Cluster Key', {})
          klass.new(attributes).update_async(new_attributes)
        end
      end

      it 'should return a future resolving to the record instance' do
        record = klass.new(partition: 'Partition Key')
        expect(record.update_async(new_attributes).get).to eq(record)
      end

      it 'should include the new attributes in the updated Record' do
        record = klass.new(partition: 'Partition Key')
        expect(record.update_async(new_attributes).get.attributes).to include(new_attributes)
      end

      context 'with different attributes' do
        let(:attributes) { {partition: 'Different Partition Key', cluster: 'Different Cluster Key'} }

        it 'should update the record in the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Some Data', 'Different Partition Key', 'Different Cluster Key', {})
          klass.new(attributes).update_async(new_attributes)
        end
      end

      context 'with a different table name' do
        let(:table_name) { :image_data }

        it 'should update the record in the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Some Data', 'Partition Key', 'Cluster Key', {})
          klass.new(attributes).update_async(new_attributes)
        end
      end

      context 'with different partition and clustering keys' do
        let(:partition_key) { [:different_partition] }
        let(:clustering_columns) { [:different_cluster] }
        let(:attributes) { {different_partition: 'Partition Key', different_cluster: 'Cluster Key'} }

        it 'should update the record in the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Some Data', 'Partition Key', 'Cluster Key', {})
          klass.new(attributes).update_async(new_attributes)
        end
      end
    end

    describe '#update' do
      let(:attributes) { {partition: 'Partition Key'} }
      let(:new_attributes) { {meta_data: 'meta-data', misc_data: 'Some additional information'} }
      let(:record) { klass.new(attributes) }
      let(:record_future) { Cassandra::Future.value(record) }

      it 'should update the record' do
        expect(record).to receive(:update_async).with(new_attributes).and_return(record_future)
        record.update(new_attributes)
      end

      it 'should resolve the future of #update_async' do
        allow(record).to receive(:update_async).with(new_attributes).and_return(record_future)
        expect(record.update(new_attributes)).to eq(record)
      end
    end

    shared_examples_for 'an inspection method' do
      let(:attributes) { {partition_key: 'Partition', clustering: 45} }
      let(:record) { klass.new(attributes, validate: false) }
      let(:cassandra_columns) { {partition_key: :text, clustering: :text} }

      before do
        allow(klass).to receive(:cassandra_columns).and_return(cassandra_columns)
        allow(klass).to receive(:normalized_column) { |column| column }
      end

      it { is_expected.to eq('#<CassandraModel::Record partition_key: "Partition", clustering: "45">') }

      context 'with a different record' do
        let(:attributes) { {partition_key: 'Different Partition', description: 'A great image!'} }
        let(:cassandra_columns) { {partition_key: :text, description: :text} }
        let(:klass) { NamedClass.create('CassandraModel::ImageData', base_record_klass) }

        it { is_expected.to eq('#<CassandraModel::ImageData partition_key: "Different Partition", description: "A great image!">') }
      end

      context 'when a column is a blob' do
        let(:cassandra_columns) { {partition_key: :text, clustering: :blob} }

        it { is_expected.to eq('#<CassandraModel::Record partition_key: "Partition">') }
      end

      context 'when the record class maps columns' do
        let(:cassandra_columns) { {rk_partition_key: :text, rk_clustering: :text} }

        before do
          allow(klass).to receive(:normalized_column) do |column|
            (column =~ /^rk_/) ? column.to_s[3..-1].to_sym : column
          end
        end

        it { is_expected.to eq('#<CassandraModel::Record partition_key: "Partition", clustering: "45">') }

        context 'when a normalized column appears twice' do
          let(:cassandra_columns) { {rk_partition_key: :text, rk_clustering: :text, partition_key: :text} }

          it { is_expected.to eq('#<CassandraModel::Record partition_key: "Partition", clustering: "45">') }
        end
      end

      context 'when some of the attributes are not assigned' do
        let(:attributes) { {partition_key: 'Different Partition'} }
        let(:cassandra_columns) { {partition_key: :text, description: :text} }

        it { is_expected.to eq('#<CassandraModel::Record partition_key: "Different Partition", description: (empty)>') }
      end

      context 'with an invalid record' do
        before { record.invalidate! }

        it { is_expected.to eq('#<CassandraModel::Record(Invalidated) partition_key: "Partition", clustering: "45">') }
      end

      context 'with a really long attribute' do
        let(:key) { 'My super awesome really long and crazy partition key of spamming your irb' }
        let(:trimmed_key) { key.truncate(53) }
        let(:cassandra_columns) { {partition_key: :text} }
        let(:attributes) { {partition_key: key} }

        it { is_expected.to eq(%Q{#<CassandraModel::Record partition_key: "#{trimmed_key}">}) }
      end

      context 'with deferred columns' do
        let(:attributes) { {} }
        let(:cassandra_columns) { {} }

        before do
          klass.deferred_column :description, on_load: ->(_) { {Faker::Lorem.word => Faker::Lorem.word} }
        end

        it { is_expected.to eq(%Q{#<CassandraModel::Record description: "#{record.description.inspect}">}) }
      end
    end

    describe '#inspect' do
      subject { record.inspect }
      it_behaves_like 'an inspection method'
    end

    describe '#to_s' do
      subject { record.to_s }
      it_behaves_like 'an inspection method'
    end

    describe '#==' do
      it 'should be true when the attributes match' do
        expect(klass.new(partition: 'Partition Key')).to eq(klass.new(partition: 'Partition Key'))
      end

      it 'should be false when the attributes do not match' do
        expect(klass.new(partition: 'Partition Key')).not_to eq(klass.new(partition: 'Different Key'))
      end

      context 'when comparing a non Record' do
        it 'should return false' do
          expect(klass.new({})).not_to eq('Record')
        end
      end

      context 'with one record having a nil value and another missing the key' do
        subject { klass.new(partition: 'some partition', cluster: nil) }

        it { is_expected.to eq(klass.new(partition: 'some partition')) }
      end

      describe 'working with deferred columns' do
        let(:lhs) { klass.new(partition: 'Partition Key', data: SecureRandom.uuid) }
        let(:rhs) { klass.new(partition: 'Partition Key', data: SecureRandom.uuid) }

        before { klass.deferred_column :data, on_load: ->(_) {} }

        it 'should not include deferred columns when comparing' do
          expect(lhs).to eq(rhs)
        end
      end
    end
  end
end
