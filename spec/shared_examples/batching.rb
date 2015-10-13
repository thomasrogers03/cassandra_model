module CassandraModel
  shared_examples_for 'a single token batch' do
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
