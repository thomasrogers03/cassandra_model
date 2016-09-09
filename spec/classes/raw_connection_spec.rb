require 'spec_helper'

module CassandraModel
  describe RawConnection do
    let(:raw_connection) { RawConnection.new }

    describe '#config' do
      subject { raw_connection.config }

      let(:config) do
        {
            hosts: %w(localhost),
            keyspace: 'default_keyspace',
            keyspace_options: {
                class: 'SimpleStrategy',
                replication_factor: 1
            },
            port: '9042',
            consistency: :one,
            connection_timeout: 10,
            timeout: 10
        }
      end

      it 'should use a default configuration' do
        is_expected.to eq(config)
      end

      context 'when config/cassandra.yml exists' do
        let(:default_config) do
          {
              hosts: %w(behemoth),
              keyspace: 'keyspace',
              keyspace_options: {
                  class: 'SimpleStrategy',
                  replication_factor: 1
              },
              port: '7777',
              consistency: :all,
              connection_timeout: 60,
              timeout: 40
          }
        end
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
          let(:default_config) do
            {
                hosts: %w(behemoth),
                keyspace: 'keyspace',
                keyspace_options: {
                    class: 'SimpleStrategy',
                    replication_factor: 1
                },
                port: '7777',
                consistency: :quorum,
                connection_timeout: 60,
                timeout: 60
            }
          end
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
              keyspace_options: {
                  class: 'SimpleStrategy',
                  replication_factor: 1
              },
              port: '9999',
              consistency: :all,
              connection_timeout: 60,
              timeout: 30
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

    describe '#executor' do
      subject { raw_connection.executor }

      it { is_expected.to be_a_kind_of(Concurrent::CachedThreadPool) }

      it 'should instance cache the value' do
        raw_connection.executor
        expect(Concurrent::CachedThreadPool).not_to receive(:new)
        raw_connection.executor
      end
    end

    describe '#futures_factory' do
      subject { raw_connection.futures_factory }

      it { is_expected.to be_a_kind_of(Cassandra::Future::Factory) }

      it 'should use our executor' do
        expect(subject.instance_variable_get(:@executor)).to eq(raw_connection.executor)
      end

      it 'should instance cache the value' do
        raw_connection.futures_factory
        expect(Cassandra::Future::Factory).not_to receive(:new)
        raw_connection.futures_factory
      end
    end

    describe '#cluster' do
      subject { raw_connection.cluster }

      let(:connection_cluster) { double(:cluster) }
      let(:config) do
        {
            hosts: %w(localhost),
            logger: Logging.logger,
            consistency: :one,
            connection_timeout: 10,
            timeout: 10,
            futures_factory: raw_connection.futures_factory
        }
      end

      before do
        allow(Cassandra).to receive(:cluster).with(hash_including(config)).and_return(connection_cluster, double(:second_connection))
      end

      it 'should create a cassandra connection with the specified configuration' do
        is_expected.to eq(connection_cluster)
      end

      context 'when a connection has already been created' do
        it 'should not create more than one connection' do
          raw_connection.cluster
          is_expected.to eq(connection_cluster)
        end
      end

      context 'with a different compression method' do
        let(:config) { {compression: :snappy} }
        before { raw_connection.config = config }

        it { is_expected.to eq(connection_cluster) }
      end

      context 'with a other options' do
        let(:config) do
          {
              compression: :snappy,
              consistency: :all,
              connection_timeout: 30,
              timeout: 15,
              address_resolution: :ec2_multi_region
          }
        end
        before { raw_connection.config = config }

        it { is_expected.to eq(connection_cluster) }
      end

      context 'with authentication' do
        let(:config) { {username: 'Greate User Tony Bobas', password: 'BackBone'} }
        before { raw_connection.config = config }

        it { is_expected.to eq(connection_cluster) }
      end
    end

    describe '#session' do
      let(:config) { {keyspace: 'keyspace'} }
      let(:session) { double(:connection) }

      before do
        raw_connection.config = config
        allow(cluster).to receive(:connect).with('keyspace').and_return(session)
      end

      it 'should connect to the cluster using the pre-configured key-space' do
        expect(raw_connection.session).to eq(session)
      end

      it 'should cache the connection' do
        raw_connection.session
        expect(cluster).not_to receive(:connect)
        raw_connection.session
      end
    end

    describe '#keyspace' do
      let(:session) { double(:connection) }

      it 'should be the keyspace object used to connect to the cluster' do
        expect(raw_connection.keyspace).to eq(keyspace)
      end

      context 'when the keyspace does not yet exist' do
        let(:cql) do
          "CREATE KEYSPACE IF NOT EXISTS default_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
        end

        before do
          allow(raw_connection).to receive(:sleep)
          allow(cluster).to receive(:connect).with(no_args).and_return(session)
          allow(cluster).to receive(:keyspace).and_return(nil)
          allow(session).to receive(:execute).with(cql) do
            allow(cluster).to receive(:keyspace).and_return(nil, nil, nil, keyspace)
          end
        end

        it 'should create the keyspace with the default options' do
          expect(raw_connection.keyspace).to eq(keyspace)
        end

        context 'with different keyspace options' do
          let(:keyspace_options) { {class: 'NetworkTopologyStrategy', dc1: 3, dc2: 4} }
          let(:keyspace_name) { 'my_keyspace' }
          let(:config) { {keyspace: keyspace_name, keyspace_options: keyspace_options} }
          let(:cql) do
            "CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 3, 'dc2' : 4 };"
          end

          before { raw_connection.config = config }

          it 'should create the keyspace with the specified options' do
            expect(raw_connection.keyspace).to eq(keyspace)
          end
        end
      end
    end

    describe '#statement' do
      let(:query) { 'SELECT * FROM everything' }
      let(:statement_query) { 'SELECT * FROM everything' }
      let!(:statement) { mock_prepare(query) }

      it 'should prepare a statement using the created connection' do
        expect(raw_connection.statement(statement_query)).to eq(statement)
      end

      it 'should cache the statement for later use' do
        raw_connection.statement(statement_query)
        expect(connection).not_to receive(:prepare)
        raw_connection.statement(statement_query)
      end

      context 'when the query is not a string' do
        let(:query_klass) do
          Struct.new(:query) do
            def to_s
              query
            end
          end
        end
        let(:statement_query) { query_klass.new(query) }

        it 'should convert it to a string first' do
          expect(raw_connection.statement(statement_query)).to eq(statement)
        end
      end
    end

    shared_examples_for 'a batch reactor' do |method, type|
      let(:batch_options) { {} }
      let(:batch_config) { {batch_reactor: batch_options} }

      before do
        raw_connection.config = batch_config
        mock_reactor(cluster, type, batch_options)
      end

      it 'should return the BatchReactor for this cluster connection' do
        expect(raw_connection.public_send(method)).to eq(global_reactor)
      end

      it 'should start the reactor' do
        expect(global_reactor.started_future).to receive(:get)
        raw_connection.public_send(method)
      end

      it 'should re-use the same reactor' do
        raw_connection.public_send(method)
        expect(BatchReactor).not_to receive(:new)
        raw_connection.public_send(method)
      end

      context 'with a max batch size configured' do
        let(:batch_options) { {max_batch_size: 25} }

        it 'should return the BatchReactor for this cluster connection with the configured options' do
          expect(raw_connection.public_send(method)).to eq(global_reactor)
        end
      end
    end

    describe('#unlogged_batch_reactor') { it_behaves_like 'a batch reactor', :unlogged_batch_reactor, SingleTokenUnloggedBatch }
    describe('#logged_batch_reactor') { it_behaves_like 'a batch reactor', :logged_batch_reactor, SingleTokenLoggedBatch }
    describe('#counter_batch_reactor') { it_behaves_like 'a batch reactor', :counter_batch_reactor, SingleTokenCounterBatch }

    describe '#shutdown' do
      let(:session) { double(:cluster, close: nil) }
      let(:cluster) { double(:cluster, connect: session, close: nil) }
      let!(:unlogged_batch_reactor) { mock_shutdown_reactor(cluster, SingleTokenUnloggedBatch) }
      let!(:logged_batch_reactor) { mock_shutdown_reactor(cluster, SingleTokenLoggedBatch) }
      let!(:counter_batch_reactor) { mock_shutdown_reactor(cluster, SingleTokenCounterBatch) }

      before do
        allow(Cassandra).to receive(:cluster).and_return(cluster)
      end

      context 'with a cluster' do
        before { raw_connection.cluster }

        it 'should close the cluster connection' do
          expect(cluster).to receive(:close)
          raw_connection.shutdown
        end

        context 'with a session' do
          before { raw_connection.session }

          it 'should close the cluster after closing the session' do
            expect(session).to receive(:close).ordered
            expect(cluster).to receive(:close).ordered
            raw_connection.shutdown
          end
        end
      end

      it 'should not close the cluster connection if it had never connected' do
        expect(cluster).not_to receive(:close)
        raw_connection.shutdown
      end

      shared_examples_for 'shutting down a reactor' do |name, type|
        let!(:reactor) { mock_shutdown_reactor(cluster, type) }

        context "with a #{type} reactor" do
          before { raw_connection.public_send(name) }

          it 'should shutdown the reactor' do
            expect(reactor.stopped_future).to receive(:get)
            raw_connection.shutdown
          end
        end

        it 'should not shutdown a reactor that was never started' do
          expect(reactor.stopped_future).not_to receive(:get)
          raw_connection.shutdown
        end

      end

      it_behaves_like 'shutting down a reactor', :unlogged_batch_reactor, SingleTokenUnloggedBatch
      it_behaves_like 'shutting down a reactor', :logged_batch_reactor, SingleTokenLoggedBatch
      it_behaves_like 'shutting down a reactor', :counter_batch_reactor, SingleTokenCounterBatch

      shared_examples_for 'a re-use attempt' do |method|
        before { raw_connection.shutdown }

        describe "calling ##{method} after shutdown" do
          it 'should raise an error' do
            expect { raw_connection.public_send(method) }.to raise_error(Cassandra::Errors::InvalidError, 'Connection invalidated!')
          end
        end
      end

      it_behaves_like 'a re-use attempt', :cluster
      it_behaves_like 'a re-use attempt', :session
      it_behaves_like 'a re-use attempt', :unlogged_batch_reactor
      it_behaves_like 'a re-use attempt', :logged_batch_reactor
      it_behaves_like 'a re-use attempt', :counter_batch_reactor
    end

    private

    def mock_shutdown_reactor(cluster, type)
      stopped_future = double(:future, get: nil)
      started_future = double(:future, get: nil)
      double(:reactor, stopped_future: stopped_future, stop: stopped_future, start: started_future).tap do |reactor|
        allow(CassandraModel::BatchReactor).to receive(:new).with(cluster, cluster.connect, type, {}).and_return(reactor)
      end
    end

  end
end
