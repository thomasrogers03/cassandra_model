require 'spec_helper'

describe Record do
  class Record
    def self.reset!
      @table_name = nil
      @save_query = nil
      @@connection = nil
      @@cluster = nil
      @@statement_cache = {}
      @@keyspace = nil
    end
  end

  class ImageData < Record
    ImageData.columns = [:partition]
  end

  class MockFuture
    def initialize(result)
      @result = result
    end

    def join

    end

    def get
      @result
    end
  end

  let(:cluster) { double(:cluster, connect: connection) }
  let(:connection) { double(:connection) }

  before do
    allow(Cassandra).to receive(:cluster).and_return(cluster)
    Record.reset!
  end

  describe '.keyspace' do
    let(:keyspace) { double(:keyspace) }

    before { allow(cluster).to receive(:keyspace).with(Record.config[:keyspace]).and_return(keyspace) }

    it 'should be the keyspace object used to connect to the cluster' do
      expect(Record.keyspace).to eq(keyspace)
    end

    it 'should cache the keyspace object' do
      Record.keyspace
      expect(cluster).not_to receive(:keyspace)
      Record.keyspace
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

  describe '.primary_key=' do
    it 'should set the primary key for the given column-family in cql style' do
      Record.primary_key = [[:partition], :cluster]
      expect(Record.primary_key).to eq([[:partition], :cluster])
    end

    context 'when only the partition key is specified' do
      it 'should map a single value to the cql definition' do
        Record.primary_key = :partition
        expect(Record.primary_key).to eq([[:partition]])
      end
    end

    context 'when the partition key is specified with a single clustering key' do
      it 'should map it to the cql definition' do
        Record.primary_key = [:partition, :cluster]
        expect(Record.primary_key).to eq([[:partition], :cluster])
      end
    end
  end

  describe '.columns=' do
    it 'should define save the column names' do
      Record.columns = [:partition, :cluster]
      expect(Record.columns).to eq([:partition, :cluster])
    end

    it 'should define an attribute accessor for each colun' do
      Record.columns = [:partition]
      record = Record.new({partition: 'Some Partition'})
      record.partition = 'Partition Key'
      expect(record.partition).to eq('Partition Key')
    end
  end

  describe '.config' do
    subject { Record.config }

    let(:config) do
      {
          hosts: %w(localhost),
          keyspace: 'default_keyspace',
          port: '9042'
      }
    end

    it 'should use a default configuration' do
      expect(subject).to eq(config)
    end

    context 'when specifying the options' do
      let (:config) do
        {
            hosts: %w(me),
            keyspace: 'new_keyspace',
            port: '9999'
        }
      end

      before { Record.config = config }

      it { expect(subject).to eq(config) }
    end

    context 'when providing a configuration with missing keys' do
      before { Record.config = {} }
      it { expect(subject).to eq(config) }
    end
  end

  describe '.cluster' do
    subject { Record.cluster }

    let(:config) { {hosts: %w(localhost), connect_timeout: 120} }

    before do
      allow(Cassandra).to receive(:cluster).with(hash_including(config)).and_return(cluster, double(:second_connection))
    end

    it 'should create a cassandra connection with the specified configuration' do
      expect(subject).to eq(cluster)
    end

    context 'when a connection has already been created' do
      it 'should not create more than one connection' do
        Record.cluster
        expect(subject).to eq(cluster)
      end
    end

    context 'with a different compression method' do
      let(:config) { {hosts: %w(localhost), connect_timeout: 120, compression: :snappy} }
      before { Record.config = {compression: 'snappy'} }

      it { should == cluster }
    end
  end

  describe '.connection' do
    let(:config) { {keyspace: 'keyspace'} }
    let(:connection) { double(:connection) }

    it 'should connect to the cluster using the pre-configured key-space' do
      allow(Cassandra).to receive(:cluster).and_return(cluster)
      allow(cluster).to receive(:connect).with('keyspace').and_return(connection)
      Record.config = config
      expect(Record.connection).to eq(connection)
    end
  end

  describe '.paginate' do
    let(:next_page) { nil }
    let(:last_page) { true }
    let(:result) { double(:result, :last_page? => last_page, next_page: next_page) }
    let(:connection) { double(:connection) }
    let(:query) { 'SELECT * FROM everything' }

    before do
      allow(Record).to receive(:connection).and_return(connection)
      allow(connection).to receive(:execute).with(query, page_size: 10).and_return(result)
    end

    it 'should yield the result of a query' do
      expect { |b| Record.paginate(query, {page_size: 10}, &b) }.to yield_with_args(result)
    end

    context 'with multiple pages' do
      let(:last_page) { false }
      let(:next_page) do
        page = double(:result, last_page?: true)
        allow(page).to receive(:next_page) { raise 'Cannot load next_page on last page' }
        page
      end

      it 'should yield all the results' do
        found_page = false
        Record.paginate(query, page_size: 10) do |page|
          found_page = page == next_page
        end
        expect(found_page).to eq(true)
      end
    end
  end

  describe '.statement' do
    let(:query) { 'SELECT * FROM everything' }
    let(:statement) { double(:statement) }

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

  describe '.create_async' do
    let(:attributes) { { partition: 'Partition Key' } }
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

  describe '.where_async' do
    let(:clause) { {} }
    let(:where_clause) { nil }
    let(:table_name) { :table }
    let(:select_clause) { '*' }
    let(:query) { "SELECT #{select_clause} FROM #{table_name}#{where_clause}" }
    let(:statement) { double(:statement) }
    let(:results) { MockFuture.new(['partition' => 'Partition Key']) }
    let(:record) { Record.new(partition: 'Partition Key') }

    before do
      Record.table_name = table_name
      Record.primary_key = [[:partition], :cluster, :time_stamp]
      Record.columns = [:partition, :cluster, :time_stamp]
      allow(Record).to receive(:statement).with(query).and_return(statement)
      allow(connection).to receive(:execute_async).and_return(results)
    end

    it 'should create a Record instance for each returned result' do
      expect(Record.where_async(clause).get.first).to eq(record)
    end

    context 'when selecting a subset of columns' do
      let(:clause) { { select: :partition} }
      let(:select_clause) { :partition }

      it 'should return a QueryResult instead of a record' do
        expect(Record.where_async(clause).get.first).to be_a_kind_of(QueryResult)
      end

      context 'with multiple columns selected' do
        let(:clause) { { select: [:partition, :cluster]} }
        let(:select_clause) { %w(partition cluster).join(', ') }
        let(:results) { MockFuture.new([{'partition' => 'Partition Key', cluster: 'Cluster Key'}]) }
        let(:record) { QueryResult.new(partition: 'Partition Key', cluster: 'Cluster Key') }

        it 'should select all the specified columns' do
          expect(Record.where_async(clause).get.first).to eq(record)
        end
      end
    end

    context 'with a different record type' do
      let(:table_name) { :image_data }

      it 'should return records of that type' do
        expect(ImageData.where_async(clause).get.first).to be_a_kind_of(ImageData)
      end
    end

    context 'with multiple results' do
      let(:clause) { {limit: 1} }
      let(:where_clause) { ' LIMIT 1' }
      let(:results) { MockFuture.new([{'partition' => 'Partition Key 1'}, {'partition' => 'Partition Key 2'}]) }

      it 'should support limits' do
        expect(connection).to receive(:execute_async).with(statement).and_return(results)
        Record.where_async(clause)
      end

      context 'with a strange limit' do
        let(:clause) { {limit: 'bob'} }

        it 'should raise an error' do
          expect { Record.where_async(clause) }.to raise_error("Invalid limit 'bob'")
        end
      end
    end

    context 'with no clause' do
      it 'should query for everything' do
        expect(connection).to receive(:execute_async).with(statement).and_return(results)
        Record.where_async(clause)
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
        expect(connection).to receive(:execute_async).with(statement, 'Partition Key').and_return(results)
        Record.where_async(clause)
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
        expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Cluster Key').and_return(results)
        Record.where_async(clause)
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
        Record.where_async(clause).each do |result|
          results << result
        end
        expected_records = [
            Record.new(partition: 'Partition Key 1'),
            Record.new(partition: 'Partition Key 2')
        ]
        expect(results).to eq(expected_records)
      end
    end
  end

  describe '.first_async' do
    let(:clause) { {partition: 'Partition Key'} }
    let(:record) { Record.new(partition: 'Partition Key') }
    let(:future_record) { MockFuture.new([record]) }

    it 'should delegate to where using a limit of 1' do
      allow(Record).to receive(:where_async).with(clause.merge(limit: 1)).and_return(future_record)
      expect(Record.first_async(clause).get).to eq(record)
    end

    it 'should default the where clause to {}' do
      expect(Record).to receive(:where_async).with(limit: 1)
      Record.first_async
    end
  end

  describe '.create' do
    let(:attributes) { { partition: 'Partition Key' } }
    let(:record) { Record.new(attributes) }
    let(:future_record) { MockFuture.new(record) }

    before do
      allow(Record).to receive(:create_async).with(attributes).and_return(future_record)
    end

    it 'should resolve the future returned by .create_async' do
      expect(Record.create(attributes)).to eq(record)
    end
  end

  describe '.where' do
    let(:clause) { {} }
    let(:record) { Record.new(partition: 'Partition Key') }
    let(:future_record) { MockFuture.new([record]) }

    it 'should resolve the future provided by where_async' do
      allow(Record).to receive(:where_async).with(clause).and_return(future_record)
      expect(Record.where(clause)).to eq([record])
    end

    context 'when paginating' do
      let(:clause) { { page_size: 3 } }

      it 'should just forward the result' do
        allow(Record).to receive(:where_async).with(clause).and_return(future_record)
        expect(Record.where(clause)).to eq(future_record)
      end
    end
  end

  describe '.first' do
    let(:clause) { {} }
    let(:record) { double(:record) }
    let(:future_record) { MockFuture.new(record) }

    it 'should resolve the future provided by first_async' do
      allow(Record).to receive(:first_async).with(clause).and_return(future_record)
      expect(Record.first(clause)).to eq(record)
    end

    it 'should default the where clause to {}' do
      expect(Record).to receive(:first_async).with({}).and_return(future_record)
      Record.first
    end
  end

  describe '#attributes' do
    before { Record.columns = [:partition] }

    it 'should return the attributes of the created Record' do
      record = Record.new(partition: 'Partition Key')
      expect(record.attributes).to eq(partition: 'Partition Key')
    end

    context 'with an invalid column' do
      it 'should raise an error' do
        expect { Record.new(fake_column: 'Partition Key') }.to raise_error("Invalid column 'fake_column' specified")
      end
    end
  end

  describe '#save_async' do
    let(:columns) { [:partition] }
    let(:attributes) { { partition: 'Partition Key' } }
    let(:query) { "INSERT INTO table (#{columns.join(', ')}) VALUES (#{(%w(?) * columns.size).join(', ')})" }
    let(:statement) { double(:statement) }
    let(:results) { MockFuture.new([]) }

    before do
      Record.table_name = :table
      Record.columns = columns
      allow(Record).to receive(:statement).with(query).and_return(statement)
      allow(connection).to receive(:execute_async).and_return(results)
    end

    it 'should save the record to the database' do
      expect(connection).to receive(:execute_async).with(statement, 'Partition Key').and_return(results)
      Record.new(attributes).save_async
    end

    it 'should return a future resolving to the record instance' do
      record = Record.new(partition: 'Partition Key')
      expect(record.save_async.get).to eq(record)
    end

    context 'with different columns' do
      let(:columns) { [:partition, :cluster] }
      let(:attributes) { { partition: 'Partition Key', cluster: 'Cluster Key' } }

      it 'should save the record to the database using the specified attributes' do
        expect(connection).to receive(:execute_async).with(statement, 'Partition Key', 'Cluster Key').and_return(results)
        Record.new(attributes).save_async
      end
    end
  end

  describe '#save' do
    let(:attributes) { { partition: 'Partition Key' } }
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

  describe '#==' do
    it 'should be true when the attributes match' do
      expect(Record.new(partition: 'Partition Key')).to eq(Record.new(partition: 'Partition Key'))
    end

    it 'should be false when the attributes do not match' do
      expect(Record.new(partition: 'Partition Key')).not_to eq(Record.new(partition: 'Different Key'))
    end
  end
end