require 'rspec'

module CassandraModel
  describe MetaTable do
    let(:connection) { double(:connection) }

    subject { MetaTable.new({}) }

    before { allow(MetaTable).to receive(:connection).and_return(connection) }

    it { is_expected.to be_a_kind_of(Record) }

    describe '.create_descriptor_table' do
      it 'should create the table in cassandra' do
        expected_query = 'CREATE TABLE meta_tables (name ascii, created_at timestamp, id ascii, PRIMARY KEY ((name), created_at, id))'
        expect(connection).to receive(:execute).with(expected_query)
        MetaTable.create_descriptor_table
      end
    end

    describe '.drop_descriptor_table' do
      it 'should drop the table from cassandra' do
        expected_query = 'DROP TABLE meta_tables'
        expect(connection).to receive(:execute).with(expected_query)
        MetaTable.drop_descriptor_table
      end
    end
  end
end