require 'spec_helper'

module CassandraModel
  describe SingleTokenLoggedBatch do
    it { is_expected.to be_a_kind_of(Cassandra::Statements::Batch::Logged) }

    it_behaves_like 'a single token batch'
  end
end
