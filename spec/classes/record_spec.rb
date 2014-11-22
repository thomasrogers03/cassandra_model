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

  describe '#connection' do
    let(:config) { {keyspace: 'keyspace'} }
    let(:connection) { double(:connection) }

    it 'should connect to the cluster using the pre-configured key-space' do
      allow(Cassandra).to receive(:cluster).and_return(cluster)
      allow(cluster).to receive(:connect).with('keyspace').and_return(connection)
      Record.config = config
      expect(Record.connection).to eq(connection)
    end
  end
end