require 'rspec'

module CassandraModel
  describe Connection do
    class TestConnection
      extend Connection
    end

    let(:cluster) { double(:cluster, connect: connection) }
    let(:connection) { double(:connection) }
    let(:column_object) { double(:column, name: 'partition') }
    let(:table_object) { double(:table, columns: [column_object]) }
    let(:keyspace) { double(:keyspace, table: table_object) }

    before do
      allow(Cassandra).to receive(:cluster).and_return(cluster)
      allow(cluster).to receive(:keyspace).and_return(keyspace)
      Connection.reset!
    end

    describe '.config' do
      subject { TestConnection.config }

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

      context 'when config/cassandra.yml exists' do
        let(:config) { {hosts: %w(behemoth), keyspace: 'keyspace', port: '7777'} }

        before do
          io = StringIO.new
          io << YAML.dump(config.stringify_keys)
          io.rewind
          allow(File).to receive(:exists?).with('./config/cassandra.yml').and_return(true)
          allow(File).to receive(:open).with('./config/cassandra.yml').and_yield(io)
        end

        it 'should load configuration from that file' do
          expect(subject).to eq(config)
        end

        context 'when providing a configuration with missing keys' do
          let(:config) {{ hosts: %w(behemoth) }}

          it { expect(subject).to eq(Connection::DEFAULT_CONFIGURATION.merge(config)) }
        end

        context 'when rails is present' do
          let(:config) { { :production => {hosts: %w(behemoth), keyspace: 'keyspace', port: '7777'}} }
          let(:rails) { double(:rails, env: 'production') }

          before { stub_const('Rails', rails) }

          it 'should load the configuration from that file with the rails environment' do
            expect(subject).to eq(config[:production])
          end
        end
      end

      context 'when specifying the options' do
        let (:config) do
          {
              hosts: %w(me),
              keyspace: 'new_keyspace',
              port: '9999'
          }
        end

        before { TestConnection.config = config }

        it { expect(subject).to eq(config) }
      end

      context 'when providing a configuration with missing keys' do
        before { TestConnection.config = {} }
        it { expect(subject).to eq(config) }
      end
    end

    describe '.cluster' do
      subject { TestConnection.cluster }

      let(:config) { {hosts: %w(localhost), connect_timeout: 120} }

      before do
        allow(Cassandra).to receive(:cluster).with(hash_including(config)).and_return(cluster, double(:second_connection))
      end

      it 'should create a cassandra connection with the specified configuration' do
        expect(subject).to eq(cluster)
      end

      context 'when a connection has already been created' do
        it 'should not create more than one connection' do
          TestConnection.cluster
          expect(subject).to eq(cluster)
        end
      end

      context 'with a different compression method' do
        let(:config) { {hosts: %w(localhost), connect_timeout: 120, compression: :snappy} }
        before { TestConnection.config = {compression: 'snappy'} }

        it { should == cluster }
      end
    end

    describe '.connection' do
      let(:config) { {keyspace: 'keyspace'} }
      let(:connection) { double(:connection) }

      before do
        TestConnection.config = config
        allow(cluster).to receive(:connect).with('keyspace').and_return(connection)
      end

      it 'should connect to the cluster using the pre-configured key-space' do
        expect(TestConnection.connection).to eq(connection)
      end

      it 'should cache the connection' do
        TestConnection.connection
        expect(cluster).not_to receive(:connect).with('keyspace')
        TestConnection.connection
      end
    end

  end
end