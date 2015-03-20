require 'rspec'

module CassandraModel
  describe MetaTable do
    subject { MetaTable.new({}) }

    it { is_expected.to be_a_kind_of(Record) }
  end
end