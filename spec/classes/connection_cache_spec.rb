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
  end
end