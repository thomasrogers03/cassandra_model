require 'rspec'

module CassandraModel
  describe SingleTokenCounterBatch do
    it { is_expected.to be_a_kind_of(Cassandra::Statements::Batch::Counter) }

    it_behaves_like 'a single token batch'
  end
end
