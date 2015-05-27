require 'rspec'

module CassandraModel
  describe RawConnection do
    let(:cluster) { double(:cluster, connect: connection) }
    let(:connection) { double(:connection) }
    let(:column_object) { double(:column, name: 'partition') }
    let(:table_object) { double(:table, columns: [column_object]) }
    let(:keyspace) { double(:keyspace, table: table_object) }
    let(:raw_connection) { RawConnection.new }

    before do
      allow(Cassandra).to receive(:cluster).and_return(cluster)
      allow(cluster).to receive(:keyspace).and_return(keyspace)
    end

    describe '.config' do
      subject { raw_connection.config }

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
        let(:default_config) { {hosts: %w(behemoth), keyspace: 'keyspace', port: '7777'} }
        let(:config) { {'default' => default_config} }

        before do
          io = StringIO.new
          io << YAML.dump(config)
          io.rewind
          allow(File).to receive(:exists?).with('./config/cassandra.yml').and_return(true)
          allow(File).to receive(:open).with('./config/cassandra.yml').and_yield(io)
        end

        it 'should load configuration from that file' do
          expect(subject).to eq(default_config)
        end

        context 'when providing a configuration with missing keys' do
          let(:default_config) { {hosts: %w(behemoth)} }

          it { expect(subject).to eq(RawConnection::DEFAULT_CONFIGURATION.merge(default_config)) }
        end

        context 'when rails is present' do
          let(:default_config) { {hosts: %w(behemoth), keyspace: 'keyspace', port: '7777', } }
          let(:counter_config) { {hosts: %w(athena), keyspace: 'counters', port: '8777'} }
          let(:config) do
            {
                'production' => {
                    'default' => default_config,
                    'counter' => counter_config
                }
            }
          end
          let(:environment) { 'production' }
          let(:rails) { double(:rails, env: environment) }

          before { stub_const('Rails', rails) }

          it 'should load the configuration from that file with the rails environment' do
            expect(subject).to eq(default_config)
          end

          context 'when no configuration exists for the current environment' do
            let(:environment) { 'test' }
            it { expect(subject).to eq(RawConnection::DEFAULT_CONFIGURATION) }
          end

          context 'when using a named connection' do
            let(:raw_connection) { RawConnection.new('counter') }

            it { expect(subject).to eq(RawConnection::DEFAULT_CONFIGURATION.merge(counter_config)) }
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

        before { raw_connection.config = config }

        it { expect(subject).to eq(config) }
      end

      context 'when providing a configuration with missing keys' do
        before { raw_connection.config = {} }
        it { expect(subject).to eq(config) }
      end
    end

    describe '.cluster' do
      subject { raw_connection.cluster }

      let(:config) { {hosts: %w(localhost), connect_timeout: 120} }

      before do
        allow(Cassandra).to receive(:cluster).with(hash_including(config)).and_return(cluster, double(:second_connection))
      end

      it 'should create a cassandra connection with the specified configuration' do
        expect(subject).to eq(cluster)
      end

      context 'when a connection has already been created' do
        it 'should not create more than one connection' do
          raw_connection.cluster
          expect(subject).to eq(cluster)
        end
      end

      context 'with a different compression method' do
        let(:config) { {hosts: %w(localhost), connect_timeout: 120, compression: :snappy} }
        before { raw_connection.config = {compression: 'snappy'} }

        it { should == cluster }
      end
    end

    describe '.connection' do
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
        expect(cluster).not_to receive(:connect).with('keyspace')
        raw_connection.connection
      end
    end

    describe '.keyspace' do
      it 'should be the keyspace object used to connect to the cluster' do
        expect(raw_connection.keyspace).to eq(keyspace)
      end
    end

  end
end