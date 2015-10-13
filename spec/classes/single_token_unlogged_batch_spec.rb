require 'rspec'

module CassandraModel
  describe SingleTokenUnloggedBatch do

    it { is_expected.to be_a_kind_of(Cassandra::Statements::Batch::Unlogged) }

    describe '#keyspace' do
      its(:keyspace) { is_expected.to be_nil }
    end

    describe '#partition_key' do
      let(:partition_key) { SecureRandom.uuid }
      let(:statement) { double(:statement, partition_key: partition_key) }

      before { subject.statements << statement }

      its(:partition_key) { is_expected.to eq(partition_key) }
    end

  end
end
