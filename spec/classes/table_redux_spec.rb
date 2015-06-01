require 'rspec'

module CassandraModel
  describe TableRedux do
    let(:connection_name) { nil }
    let(:table_name) { :records }

    subject { TableRedux.new(connection_name, table_name) }

    describe '#name' do
      its(:name) { is_expected.to eq(:records) }

      context 'with a different name' do
        let(:table_name) { :images }
        its(:name) { is_expected.to eq(:images) }
      end
    end

    describe '#connection' do
      it 'should be the cached cassandra connection' do
        expect(subject.connection).to eq(ConnectionCache[nil].connection)
      end

      context 'with a different connection name' do
        let(:connection_name) { :counters }
        let(:hosts) { %w(cassandra.one cassandra.two) }
        let!(:connection) { mock_connection(hosts, 'keyspace') }

        before { ConnectionCache[:counters].config = {hosts: hosts, keyspace: 'keyspace'} }

        it 'should use the specified connection' do
          expect(subject.connection).to eq(ConnectionCache[:counters].connection)
        end
      end
    end
  end
end