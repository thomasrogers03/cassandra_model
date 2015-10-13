require 'rspec'

module CassandraModel
  describe SingleTokenUnloggedBatch do
    it { is_expected.to be_a_kind_of(Cassandra::Statements::Batch::Unlogged) }

    it_behaves_like 'a single token batch'
  end
end
