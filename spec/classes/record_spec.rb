require 'spec_helper'

module CassandraModel
  describe Record do
    class Record
      def self.reset!
        @table_name = nil
        @save_query = nil
        @delete_qeury = nil
        @partition_key = nil
        @clustering_columns = nil
        @columns = nil

        @deferred_column_writers = nil
        @async_deferred_column_readers = nil
        @async_deferred_column_writers = nil

        Connection.reset!
      end

      def self.columns=(columns)
        @columns = columns
      end
    end

    class ImageData < Record
      ImageData.columns = [:partition]
    end

    let(:cluster) { double(:cluster, connect: connection) }
    let(:connection) { double(:connection) }
    let(:column_object) { double(:column, name: 'partition') }
    let(:table_object) { double(:table, columns: [column_object]) }
    let(:keyspace) { double(:keyspace, table: table_object) }
    let(:statement) { double(:statement) }

    before do
      allow(Cassandra).to receive(:cluster).and_return(cluster)
      allow(Concurrent::Future).to receive(:execute) do |&block|
        result = block.call
        double(:future, value: result)
      end
      Record.columns = [:partition, :cluster]
      Record.reset!
      ImageData.reset!
      ImageData.columns = [:partition, :cluster]
      allow(cluster).to receive(:keyspace).with(Record.config[:keyspace]).and_return(keyspace)
    end

    it_behaves_like 'a model with a connection', Record

    context 'when mixing in query methods' do
      subject { Record }

      before do
        Record.deferred_column :fake_column, on_load: ->(attributes) {}, on_save: ->(attributes, value) {}
        Record.async_deferred_column :async_fake_column, on_load: ->(attributes) {}, on_save: ->(attributes, value) { MockFuture.new(nil) }
        allow(Record).to receive(:statement).and_return(statement)
        allow(connection).to receive(:execute_async).and_return(MockFuture.new('OK'))
      end

      it_behaves_like 'a query helper'
      it_behaves_like 'a record defining meta columns'
    end

    describe '.reset_local_schema!' do
      let(:partition_key) { double(:column, name: 'partition') }
      let(:clustering_column) { double(:column, name: 'clustering') }
      let(:columns) { [partition_key, clustering_column] }
      let(:updated_partition_key) { double(:column, name: 'updated_partition') }
      let(:updated_clustering_column) { double(:column, name: 'updated_clustering') }
      let(:updated_columns) { [updated_partition_key, updated_clustering_column] }
      let(:table_object) do
        table = double(:table)
        allow(table).to receive(:partition_key).and_return([partition_key], [updated_partition_key])
        allow(table).to receive(:clustering_columns).and_return([clustering_column], [updated_clustering_column])
        allow(table).to receive(:columns).and_return(columns, updated_columns)
        table
      end

      subject { Record }

      before do
        Record.partition_key
        Record.clustering_columns
        Record.columns
        Record.reset_local_schema!
      end

      describe 'updating the schema with the new table structure' do
        its(:partition_key) { is_expected.to eq([:updated_partition]) }
        its(:clustering_columns) { is_expected.to eq([:updated_clustering]) }
        its(:columns) { is_expected.to eq([:updated_partition, :updated_clustering]) }
      end
    end

    shared_examples_for 'a set of columns' do |method|
      let(:column) { double(:column, name: 'partition') }
      let(:table) { double(:table, method => [column]) }
      let(:table_name) { 'records' }
      let(:keyspace) do
        keyspace = double(:keyspace)
        allow(keyspace).to receive(:table).with(table_name).and_return(table)
        keyspace
      end

      it 'should be the partition key for this table' do
        expect(Record.send(method)).to eq([:partition])
      end

      it 'should cache the partition key' do
        Record.send(method)
        expect(keyspace).not_to receive(:table)
        Record.send(method)
      end

      context 'with a different table name' do
        let(:table_name) { 'image_data' }

        it 'should be the partition key for that table' do
          expect(ImageData.send(method)).to eq([:partition])
        end
      end

      context 'with multiple partition key parts' do
        let(:other_column) { double(:column, name: 'partition_part_two') }
        let(:table) { double(:table, method => [column, other_column]) }

        it 'should be the partition key for this table' do
          expect(Record.send(method)).to eq([:partition, :partition_part_two])
        end
      end
    end

    it_behaves_like 'a set of columns', :partition_key
    it_behaves_like 'a set of columns', :clustering_columns

    describe '.columns' do
      before do
        Record.columns = nil
        ImageData.columns = nil
      end

      it_behaves_like 'a set of columns', :columns

      describe 'defining methods for record columns' do
        let(:column_object) { double(:column, name: 'partition') }
        let(:table_object) { double(:table, columns: [column_object]) }

        it 'should define a method to assign and retrieve the column' do
          record = Record.new(partition: 'Partition Key')
          record.partition = 'Different Key'
          expect(record.partition).to eq('Different Key')
        end

        context 'with multiple columns' do
          let(:other_column_object) { double(:column, name: 'clustering') }
          let(:table_object) { double(:table, columns: [column_object, other_column_object]) }

          it 'should define a method to assign and retrieve the additional column' do
            record = Record.new(clustering: 'Clustering Key')
            record.clustering = 'Different Key'
            expect(record.clustering).to eq('Different Key')
          end
        end
      end
    end

    describe '.table_name' do
      it 'should be the lower-case plural of the class' do
        expect(Record.table_name).to eq('records')
      end

      context 'when inherited from a different class' do
        it { expect(ImageData.table_name).to eq('image_data') }
      end

      context 'when overridden' do
        before { Record.table_name = 'image_data' }
        it { expect(Record.table_name).to eq('image_data') }
      end
    end

    describe '.statement' do
      let(:query) { 'SELECT * FROM everything' }

      before { allow(connection).to receive(:prepare).with(query).and_return(statement) }

      it 'should prepare a statement using the created connection' do
        expect(Record.statement(query)).to eq(statement)
      end

      it 'should cache the statement for later use' do
        Record.statement(query)
        expect(connection).not_to receive(:prepare)
        Record.statement(query)
      end
    end

    describe '.query_for_save' do
      let(:columns) { [:partition] }
      let(:klass) { Record }

      before do
        klass.table_name = nil
        klass.instance_variable_set(:@save_query, nil)
        klass.columns = columns
      end

      it 'should represent the query for saving all the column values' do
        expect(klass.query_for_save).to eq('INSERT INTO records (partition) VALUES (?)')
      end

      it 'should cache the query' do
        klass.query_for_save
        expect(klass.instance_variable_get(:@save_query)).to eq('INSERT INTO records (partition) VALUES (?)')
      end

      context 'with different columns' do
        let(:columns) { [:partition, :cluster] }

        it 'should represent the query for saving all the column values' do
          expect(klass.query_for_save).to eq('INSERT INTO records (partition, cluster) VALUES (?, ?)')
        end
      end

      context 'with a different record type/table name' do
        let(:klass) { ImageData }

        it 'should represent the query for saving all the column values' do
          expect(klass.query_for_save).to eq('INSERT INTO image_data (partition) VALUES (?)')
        end
      end
    end

    describe '.query_for_delete' do
      let(:partition_key) { [:partition] }
      let(:clustering_columns) { [] }
      let(:klass) { Record }

      before do
        klass.table_name = nil
        klass.columns = partition_key + clustering_columns
        allow(klass).to receive(:partition_key).and_return(partition_key)
        allow(klass).to receive(:clustering_columns).and_return(clustering_columns)
      end

      it 'should represent the query for saving all the column values' do
        expect(klass.query_for_delete).to eq('DELETE FROM records WHERE partition = ?')
      end

      it 'should cache the query' do
        klass.query_for_delete
        expect(klass.instance_variable_get(:@delete_qeury)).to eq('DELETE FROM records WHERE partition = ?')
      end

      context 'with different columns' do
        let(:clustering_columns) { [:cluster] }

        it 'should represent the query for deleting all the column values' do
          expect(klass.query_for_delete).to eq('DELETE FROM records WHERE partition = ? AND cluster = ?')
        end
      end

      context 'with a different record type/table name' do
        let(:klass) { ImageData }

        it 'should represent the query for saving all the column values' do
          expect(klass.query_for_delete).to eq('DELETE FROM image_data WHERE partition = ?')
        end
      end
    end

    describe '.create_async' do
      let(:attributes) { {partition: 'Partition Key'} }
      let(:klass) { Record }
      let(:record) { klass.new(attributes) }
      let(:future_record) { MockFuture.new(record) }

      before do
        allow_any_instance_of(Record).to receive(:save_async).and_return(future_record)
      end

      it 'should return a new record instance with the specified attributes' do
        expect(Record.create_async(attributes).get).to eq(record)
      end

      context 'with a different record type' do
        let(:klass) { ImageData }

        it 'should create an instance of that record' do
          expect(ImageData).to receive(:new).with(attributes).and_return(record)
          ImageData.create_async(attributes)
        end
      end
    end

    describe '.request_async' do
      let(:clause) { {} }
      let(:where_clause) { nil }
      let(:table_name) { :table }
      let(:select_clause) { '*' }
      let(:order_clause) { nil }
      let(:query) { "SELECT #{select_clause} FROM #{table_name}#{where_clause}#{order_clause}" }
      let(:page_results) { ['partition' => 'Partition Key'] }
      let(:result_page) { MockPage.new(true, MockFuture.new([]), page_results) }
      let(:results) { MockFuture.new(result_page) }
      let(:record) { Record.new(partition: 'Partition Key') }

      before do
        Record.table_name = table_name
        Record.columns = [:partition, :cluster, :time_stamp]
        allow(Record).to receive(:statement).with(query).and_return(statement)
        allow(connection).to receive(:execute_async).and_return(results)
      end

      it 'should create a Record instance for each returned result' do
        expect(Record.request_async(clause).get.first).to eq(record)
      end

      context 'when the restriction key is a KeyComparer' do
        let(:clause) { {:partition.gt => 'Partition Key'} }
        let(:where_clause) { ' WHERE partition > ?' }

        it 'should query using the specified comparer' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', {}).and_return(results)
          Record.request_async(clause)
        end
      end

      context 'when restricting by multiple values' do
        let(:clause) { {partition: ['Partition Key', 'Other Partition Key']} }
        let(:where_clause) { ' WHERE partition IN (?, ?)' }
        let(:results) { MockFuture.new([{'partition' => 'Partition Key'}, {'partition' => 'Other Partition Key'}]) }

        it 'should query using an IN' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Other Partition Key', {}).and_return(results)
          Record.request_async(clause)
        end
      end

      context 'when selecting a subset of columns' do
        let(:clause) { {select: :partition} }
        let(:select_clause) { :partition }

        it 'should return a QueryResult instead of a record' do
          expect(Record.request_async({}, clause).get.first).to be_a_kind_of(QueryResult)
        end

        context 'with multiple columns selected' do
          let(:clause) { {select: [:partition, :cluster]} }
          let(:select_clause) { %w(partition cluster).join(', ') }
          let(:page_results) { [{'partition' => 'Partition Key', cluster: 'Cluster Key'}] }
          let(:record) { QueryResult.new(partition: 'Partition Key', cluster: 'Cluster Key') }

          it 'should select all the specified columns' do
            expect(Record.request_async({}, clause).get.first).to eq(record)
          end
        end
      end

      context 'when ordering by a subset of columns' do
        let(:clause) { {order_by: :cluster} }
        let(:order_clause) { ' ORDER BY cluster' }
        let(:page_results) do
          [
              {'partition' => 'Partition Key', cluster: 'Cluster Key', other_cluster: 'Other Cluster Key'},
              {'partition' => 'Partition Key', cluster: 'Cluster Key 2', other_cluster: 'Other Cluster Key'},
              {'partition' => 'Partition Key', cluster: 'Cluster Key 2', other_cluster: 'Other Cluster Key 2'}
          ]
        end
        let(:record_one) { Record.new(partition: 'Partition Key', cluster: 'Cluster Key', other_cluster: 'Other Cluster Key') }
        let(:record_two) { Record.new(partition: 'Partition Key', cluster: 'Cluster Key 2', other_cluster: 'Other Cluster Key') }
        let(:record_three) { Record.new(partition: 'Partition Key', cluster: 'Cluster Key 2', other_cluster: 'Other Cluster Key 2') }

        before do
          Record.columns = [:partition, :cluster, :other_cluster]
        end

        it 'should order the results by the specified column' do
          expect(Record.request_async({}, clause).get).to eq([record_one, record_two, record_three])
        end

        context 'with multiple columns selected' do
          let(:clause) { {order_by: [:cluster, :other_cluster]} }
          let(:order_clause) { ' ORDER BY cluster, other_cluster' }

          it 'should order by all the specified columns' do
            expect(Record.request_async({}, clause).get).to eq([record_one, record_two, record_three])
          end
        end
      end

      context 'with a different record type' do
        let(:table_name) { :image_data }

        it 'should return records of that type' do
          expect(ImageData.request_async(clause).get.first).to be_a_kind_of(ImageData)
        end
      end

      context 'with multiple results' do
        let(:clause) { {limit: 1} }
        let(:where_clause) { ' LIMIT 1' }
        let(:results) { MockFuture.new([{'partition' => 'Partition Key 1'}, {'partition' => 'Partition Key 2'}]) }

        it 'should support limits' do
          expect(connection).to receive(:execute_async).with(statement, {}).and_return(results)
          Record.request_async({}, clause)
        end

        context 'with a strange limit' do
          let(:clause) { {limit: 'bob'} }

          it 'should raise an error' do
            expect { Record.request_async({}, clause) }.to raise_error("Invalid limit 'bob'")
          end
        end
      end

      context 'with no clause' do
        it 'should query for everything' do
          expect(connection).to receive(:execute_async).with(statement, {}).and_return(results)
          Record.request_async(clause)
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
          Record.request_async(clause)
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
          Record.request_async(clause)
        end
      end

      context 'when paginating over results' do
        let(:clause) { {page_size: 2} }
        let(:first_page_results) { [{'partition' => 'Partition Key 1'}, {'partition' => 'Partition Key 2'}] }
        let(:first_page) { MockPage.new(true, nil, first_page_results) }
        let(:first_page_future) { double(:result, get: first_page) }

        it 'should return an enumerable capable of producing all the records' do
          allow(connection).to receive(:execute_async).with(statement, page_size: 2).and_return(first_page_future)
          results = []
          Record.request_async({}, clause).each do |result|
            results << result
          end
          expected_records = [
              Record.new(partition: 'Partition Key 1'),
              Record.new(partition: 'Partition Key 2')
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
          Record.request_async(clause, options)
        end
      end
    end

    describe '.first_async' do
      let(:clause) { {partition: 'Partition Key'} }
      let(:options) { {select: :partition} }
      let(:record) { Record.new(partition: 'Partition Key') }
      let(:future_record) { MockFuture.new([record]) }

      it 'should delegate to request using a limit of 1' do
        allow(Record).to receive(:request_async).with(clause, options.merge(limit: 1)).and_return(future_record)
        expect(Record.first_async(clause, options).get).to eq(record)
      end

      it 'should default the request clause to {}' do
        expect(Record).to receive(:request_async).with({}, limit: 1)
        Record.first_async
      end
    end

    describe '.create' do
      let(:attributes) { {partition: 'Partition Key'} }
      let(:record) { Record.new(attributes) }
      let(:future_record) { MockFuture.new(record) }

      before do
        allow(Record).to receive(:create_async).with(attributes).and_return(future_record)
      end

      it 'should resolve the future returned by .create_async' do
        expect(Record.create(attributes)).to eq(record)
      end
    end

    describe '.request' do
      let(:clause) { {} }
      let(:options) { {limit: 1} }
      let(:record) { Record.new(partition: 'Partition Key') }
      let(:future_record) { MockFuture.new([record]) }

      it 'should resolve the future provided by request_async' do
        allow(Record).to receive(:request_async).with(clause, options).and_return(future_record)
        expect(Record.request(clause, options)).to eq([record])
      end

      context 'when paginating' do
        let(:options) { {page_size: 3} }

        it 'should just forward the result' do
          allow(Record).to receive(:request_async).with(clause, options).and_return(future_record)
          expect(Record.request(clause, options)).to eq(future_record)
        end
      end
    end

    describe '.first' do
      let(:clause) { {} }
      let(:options) { {select: :partition} }
      let(:record) { double(:record) }
      let(:future_record) { MockFuture.new(record) }

      it 'should resolve the future provided by first_async' do
        allow(Record).to receive(:first_async).with(clause, options).and_return(future_record)
        expect(Record.first(clause, options)).to eq(record)
      end

      it 'should default the request clause to {}' do
        expect(Record).to receive(:first_async).with({}, {}).and_return(future_record)
        Record.first
      end
    end

    describe '#attributes' do
      before { Record.columns = [:partition] }

      it 'should be a valid record initially' do
        record = Record.new(partition: 'Partition Key')
        expect(record.valid).to eq(true)
      end

      it 'should return the attributes of the created Record' do
        record = Record.new(partition: 'Partition Key')
        expect(record.attributes).to eq(partition: 'Partition Key')
      end

      context 'with an invalid column' do
        it 'should raise an error' do
          expect { Record.new(fake_column: 'Partition Key') }.to raise_error("Invalid column 'fake_column' specified")
        end

        context 'when validation is disabled' do
          it 'should not raise an error' do
            expect { Record.new({fake_column: 'Partition Key'}, validate: false) }.not_to raise_error
          end
        end
      end
    end

    describe '#save_async' do
      let(:columns) { [:partition] }
      let(:attributes) { {partition: 'Partition Key'} }
      let(:query) { "INSERT INTO table (#{columns.join(', ')}) VALUES (#{(%w(?) * columns.size).join(', ')})" }
      let(:results) { MockFuture.new([]) }

      before do
        Record.table_name = :table
        Record.columns = columns
        allow(Record).to receive(:statement).with(query).and_return(statement)
        allow(connection).to receive(:execute_async).and_return(results)
      end

      context 'when the Record class has deferred columns' do
        before do
          allow(Record).to receive(:statement).and_return(statement)
          Record.deferred_column :fake_column, on_load: ->(attributes) {}, on_save: ->(attributes, value) {}
          Record.async_deferred_column :async_fake_column, on_load: ->(attributes) {}, on_save: ->(attributes, value) {}
        end

        it 'should wrap everything in a future' do
          expect(Record.new(attributes).save_async).to be_a_kind_of(ThomasUtils::Future)
        end
      end

      context 'when the Record class does not have deferred columns' do
        it 'should return the wrapped cassandra future' do
          expect(Record.new(attributes).save_async).to be_a_kind_of(ThomasUtils::FutureWrapper)
        end
      end

      context 'when the record has been invalidated' do
        before { allow_any_instance_of(Record).to receive(:valid).and_return(false) }

        it 'should raise an error' do
          expect { Record.new(attributes).save_async }.to raise_error('Cannot save invalidated record!')
        end
      end

      it 'should save the record to the database' do
        expect(connection).to receive(:execute_async).with(statement, 'Partition Key', {}).and_return(results)
        Record.new(attributes).save_async
      end

      it 'should return a future resolving to the record instance' do
        record = Record.new(partition: 'Partition Key')
        expect(record.save_async.get).to eq(record)
      end

      context 'with different columns' do
        let(:columns) { [:partition, :cluster] }
        let(:attributes) { {partition: 'Partition Key', cluster: 'Cluster Key'} }

        it 'should save the record to the database using the specified attributes' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Cluster Key', {}).and_return(results)
          Record.new(attributes).save_async
        end
      end
    end

    describe '#save' do
      let(:attributes) { {partition: 'Partition Key'} }
      let(:record) { Record.new(attributes) }
      let(:record_future) { MockFuture.new(record) }

      it 'should save the record' do
        expect(record).to receive(:save_async).and_return(record_future)
        record.save
      end

      it 'should resolve the future of #save_async' do
        allow(record).to receive(:save_async).and_return(record_future)
        expect(record.save).to eq(record)
      end
    end

    describe '#invalidate!' do
      it 'should invalidate the Record' do
        record = Record.new({})
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
      let(:results) { MockFuture.new([]) }

      before do
        Record.table_name = table_name
        Record.columns = partition_key + clustering_columns
        allow(Record).to receive(:partition_key).and_return(partition_key)
        allow(Record).to receive(:clustering_columns).and_return(clustering_columns)
        allow(Record).to receive(:statement).with(query).and_return(statement)
        allow(connection).to receive(:execute_async).and_return(results)
      end

      it 'should delete the record from the database' do
        expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Cluster Key', {})
        Record.new(attributes).delete_async
      end

      it 'should return a future resolving to the record instance' do
        record = Record.new(partition: 'Partition Key')
        expect(record.delete_async.get).to eq(record)
      end

      it 'should invalidate the record instance' do
        record = Record.new(partition: 'Partition Key')
        record.delete_async
        expect(record.valid).to eq(false)
      end

      context 'with different attributes' do
        let(:attributes) { {partition: 'Different Partition Key', cluster: 'Different Cluster Key'} }

        it 'should delete the record from the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Different Partition Key', 'Different Cluster Key', {})
          Record.new(attributes).delete_async
        end
      end

      context 'with a different table name' do
        let(:table_name) { :image_data }

        it 'should delete the record from the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Cluster Key', {})
          Record.new(attributes).delete_async
        end
      end

      context 'with different partition and clustering keys' do
        let(:partition_key) { [:different_partition] }
        let(:clustering_columns) { [:different_cluster] }
        let(:attributes) { {different_partition: 'Partition Key', different_cluster: 'Cluster Key'} }

        it 'should delete the record from the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Cluster Key', {})
          Record.new(attributes).delete_async
        end
      end
    end

    describe '#delete' do
      let(:attributes) { {partition: 'Partition Key'} }
      let(:record) { Record.new(attributes) }
      let(:record_future) { MockFuture.new(record) }

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
      let(:extra_columns) { [:meta_data, :misc_data] }
      let(:attributes) { {partition: 'Partition Key', cluster: 'Cluster Key'} }
      let(:table_name) { :table }
      let(:where_clause) { (partition_key + clustering_columns).map { |column| "#{column} = ?" }.join(' AND ') }
      let(:new_attributes) { {meta_data: 'Some Data'} }
      let(:query) { "UPDATE #{table_name} SET meta_data = ? WHERE #{where_clause}" }
      let(:results) { MockFuture.new([]) }

      before do
        Record.table_name = table_name
        Record.columns = partition_key + clustering_columns + extra_columns
        allow(Record).to receive(:partition_key).and_return(partition_key)
        allow(Record).to receive(:clustering_columns).and_return(clustering_columns)
        allow(Record).to receive(:statement).with(query).and_return(statement)
        allow(connection).to receive(:execute_async).and_return(results)
      end

      it 'should update the record in the database' do
        expect(connection).to receive(:execute_async).with(statement, 'Some Data', 'Partition Key', 'Cluster Key', {})
        Record.new(attributes).update_async(new_attributes)
      end

      context 'with an invalid column' do
        let(:new_attributes) { {fake_column: 'Some Fake Data'} }

        it 'should raise an error' do
          expect { Record.new(attributes).update_async(new_attributes) }.to raise_error("Invalid column 'fake_column' specified")
        end
      end

      context 'when updating a single key of a map' do
        let(:new_attributes) { {:meta_data.index('Location') => 'North America'} }
        let(:query) { "UPDATE #{table_name} SET meta_data['Location'] = ? WHERE #{where_clause}" }

        it 'should update only the value of that key for the map' do
          expect(connection).to receive(:execute_async).with(statement, 'North America', 'Partition Key', 'Cluster Key', {})
          Record.new(attributes).update_async(new_attributes)
        end
      end

      context 'with multiple new attributes' do
        let(:new_attributes) { {meta_data: 'meta-data', misc_data: 'Some additional information'} }
        let(:query) { "UPDATE #{table_name} SET meta_data = ? AND misc_data = ? WHERE #{where_clause}" }

        it 'should update the record in the database with those attributes' do
          expect(connection).to receive(:execute_async).with(statement, 'meta-data', 'Some additional information', 'Partition Key', 'Cluster Key', {})
          Record.new(attributes).update_async(new_attributes)
        end
      end

      it 'should return a future resolving to the record instance' do
        record = Record.new(partition: 'Partition Key')
        expect(record.update_async(new_attributes).get).to eq(record)
      end

      it 'should include the new attributes in the updated Record' do
        record = Record.new(partition: 'Partition Key')
        expect(record.update_async(new_attributes).get.attributes).to include(new_attributes)
      end

      context 'with different attributes' do
        let(:attributes) { {partition: 'Different Partition Key', cluster: 'Different Cluster Key'} }

        it 'should update the record in the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Some Data', 'Different Partition Key', 'Different Cluster Key', {})
          Record.new(attributes).update_async(new_attributes)
        end
      end

      context 'with a different table name' do
        let(:table_name) { :image_data }

        it 'should update the record in the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Some Data', 'Partition Key', 'Cluster Key', {})
          Record.new(attributes).update_async(new_attributes)
        end
      end

      context 'with different partition and clustering keys' do
        let(:partition_key) { [:different_partition] }
        let(:clustering_columns) { [:different_cluster] }
        let(:attributes) { {different_partition: 'Partition Key', different_cluster: 'Cluster Key'} }

        it 'should update the record in the database' do
          expect(connection).to receive(:execute_async).with(statement, 'Some Data', 'Partition Key', 'Cluster Key', {})
          Record.new(attributes).update_async(new_attributes)
        end
      end
    end

    describe '#update' do
      let(:attributes) { {partition: 'Partition Key'} }
      let(:new_attributes) { {meta_data: 'meta-data', misc_data: 'Some additional information'} }
      let(:record) { Record.new(attributes) }
      let(:record_future) { MockFuture.new(record) }

      it 'should update the record' do
        expect(record).to receive(:update_async).with(new_attributes).and_return(record_future)
        record.update(new_attributes)
      end

      it 'should resolve the future of #update_async' do
        allow(record).to receive(:update_async).with(new_attributes).and_return(record_future)
        expect(record.update(new_attributes)).to eq(record)
      end
    end

    describe '#==' do
      it 'should be true when the attributes match' do
        expect(Record.new(partition: 'Partition Key')).to eq(Record.new(partition: 'Partition Key'))
      end

      it 'should be false when the attributes do not match' do
        expect(Record.new(partition: 'Partition Key')).not_to eq(Record.new(partition: 'Different Key'))
      end
    end
  end
end