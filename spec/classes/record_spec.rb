require 'spec_helper'

describe Record do
  class Record
    def self.reset!
      @@connection = nil
      @@cluster = nil
    end
  end

  class ImageData < Record
  end

  let(:cluster) { double(:cluster) }

  before { Record.reset! }

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

      before { Record.config = config  }

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

  describe '.where' do
    xit 'should return the result of a select query given a restriction' do

    end
  end
end