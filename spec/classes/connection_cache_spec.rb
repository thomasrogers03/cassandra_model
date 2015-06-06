require 'rspec'

module CassandraModel
  describe ConnectionCache do
    before { ConnectionCache.reset! }

    describe '.[]' do
      let(:connection_name) { 'default' }

      subject { ConnectionCache[connection_name] }

      it { is_expected.to be_a_kind_of(RawConnection) }

      it 'should store the configuration of the requested connection name' do
        subject.config = { hosts: %w(behemoth) }
        expect(subject.config).to eq(RawConnection::DEFAULT_CONFIGURATION.merge(hosts: %w(behemoth)))
      end

      it 'should cache the configuration' do
        expect(subject).to eql(ConnectionCache[connection_name])
      end

      context 'with multiple different connections' do
        it 'should treat them separately' do
          subject.config = { hosts: %w(behemoth) }
          ConnectionCache['counters'].config = { hosts: %w(athena) }
          expect(ConnectionCache['counters'].config).to eq(RawConnection::DEFAULT_CONFIGURATION.merge(hosts: %w(athena)))
        end
      end
    end

    describe '.clear' do
      let(:other_cluster) { double(:cluster, close: nil) }

      before do
        ConnectionCache[nil]
        ConnectionCache['counters'].config = { hosts: %w(athena) }
        allow(Cassandra).to receive(:cluster).with(hash_including(hosts: %w(athena))).and_return(other_cluster)
      end

      it 'should close all active connections' do
        expect(cluster).to receive(:close)
        expect(other_cluster).to receive(:close)
        ConnectionCache.clear
      end

      it 'should clear the connection cache' do
        prev_connection = ConnectionCache[nil]
        ConnectionCache.clear
        expect(ConnectionCache[nil]).not_to eq(prev_connection)
      end
    end

  end
end