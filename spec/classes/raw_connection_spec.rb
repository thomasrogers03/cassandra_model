require 'rspec'

module CassandraModel
  describe RawConnection do
    let(:raw_connection) { RawConnection.new }

    describe '#config' do
      subject { raw_connection.config }

      let(:config) do
        {
            hosts: %w(localhost),
            keyspace: 'default_keyspace',
            port: '9042'
        }
      end

      it 'should use a default configuration' do
        is_expected.to eq(config)
      end

      context 'when config/cassandra.yml exists' do
        let(:default_config) { {hosts: %w(behemoth), keyspace: 'keyspace', port: '7777'} }
        let(:config) { default_config }
        let(:path) { 'cassandra.yml' }

        before do
          io = StringIO.new
          io << YAML.dump(config)
          io.rewind
          allow(File).to receive(:exists?).with("./config/#{path}").and_return(true)
          allow(File).to receive(:open).with("./config/#{path}").and_yield(io)
        end

        it 'should load configuration from that file' do
          is_expected.to eq(default_config)
        end

        context 'when providing a configuration with missing keys' do
          let(:default_config) { {hosts: %w(behemoth)} }

          it { is_expected.to eq(RawConnection::DEFAULT_CONFIGURATION.merge(default_config)) }
        end

        context 'when rails is present' do
          let(:default_config) { {hosts: %w(behemoth), keyspace: 'keyspace', port: '7777', } }
          let(:config) { {'production' => default_config} }
          let(:environment) { 'production' }
          let(:rails) { double(:rails, env: environment) }

          before { stub_const('Rails', rails) }

          it 'should load the configuration from that file with the rails environment' do
            is_expected.to eq(default_config)
          end

          context 'when no configuration exists for the current environment' do
            let(:environment) { 'test' }
            it { is_expected.to eq(RawConnection::DEFAULT_CONFIGURATION) }
          end
        end

        context 'when using a named connection' do
          let(:raw_connection) { RawConnection.new('counter') }
          let(:counter_config) { {hosts: %w(athena), keyspace: 'counters', port: '8777'} }
          let(:path) { 'cassandra/counter.yml' }
          let(:config) { counter_config }

          it { is_expected.to eq(RawConnection::DEFAULT_CONFIGURATION.merge(counter_config)) }
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

        before { raw_connection.config = config }

        it { is_expected.to eq(config) }
      end

      context 'when providing a configuration with missing keys' do
        before { raw_connection.config = {} }
        it { is_expected.to eq(config) }
      end
    end

    describe '#cluster' do
      subject { raw_connection.cluster }

      let(:config) { {hosts: %w(localhost), connect_timeout: 120} }

      before do
        allow(Cassandra).to receive(:cluster).with(hash_including(config)).and_return(cluster, double(:second_connection))
      end

      it 'should create a cassandra connection with the specified configuration' do
        is_expected.to eq(cluster)
      end

      context 'when a connection has already been created' do
        it 'should not create more than one connection' do
          raw_connection.cluster
          is_expected.to eq(cluster)
        end
      end

      context 'with a different compression method' do
        let(:config) { {hosts: %w(localhost), connect_timeout: 120, compression: :snappy} }
        before { raw_connection.config = {compression: 'snappy'} }

        it { should == cluster }
      end
    end

    describe '#connection' do
      let(:config) { {keyspace: 'keyspace'} }
      let(:connection) { double(:connection) }

      before do
        raw_connection.config = config
        allow(cluster).to receive(:connect).with('keyspace').and_return(connection)
      end

      it 'should connect to the cluster using the pre-configured key-space' do
        expect(raw_connection.connection).to eq(connection)
      end

      it 'should cache the connection' do
        raw_connection.connection
        expect(cluster).not_to receive(:connect)
        raw_connection.connection
      end
    end

    describe '#keyspace' do
      it 'should be the keyspace object used to connect to the cluster' do
        expect(raw_connection.keyspace).to eq(keyspace)
      end
    end

    describe '#statement' do
      let(:query) { 'SELECT * FROM everything' }
      let!(:statement) { mock_prepare(query) }

      it 'should prepare a statement using the created connection' do
        expect(raw_connection.statement(query)).to eq(statement)
      end

      it 'should cache the statement for later use' do
        raw_connection.statement(query)
        expect(connection).not_to receive(:prepare)
        raw_connection.statement(query)
      end
    end

  end
end