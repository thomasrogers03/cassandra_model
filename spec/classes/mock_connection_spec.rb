require 'rspec'
require 'cassandra_mocks'
require 'cassandra_model/mock_connection'

module CassandraModel
  describe RawConnection do
    let(:config) { {keyspace: Faker::Lorem.word} }
    let(:connection) { RawConnection.new }

    subject { connection }

    before { connection.config = config }

    describe '#cluster' do
      its(:cluster) { is_expected.to be_a_kind_of(Cassandra::Mocks::Cluster) }

      it 'should ensure that the defined keyspace has been created' do
        expect(connection.cluster.keyspace(config[:keyspace])).to be_a_kind_of(Cassandra::Mocks::Keyspace)
      end
    end

    describe '#session' do
      subject { connection.session }

      it { is_expected.to be_a_kind_of(Cassandra::Mocks::Session) }
      its(:keyspace) { is_expected.to eq(config[:keyspace]) }
      its(:cluster) { is_expected.to eq(connection.cluster) }
    end
  end
end
