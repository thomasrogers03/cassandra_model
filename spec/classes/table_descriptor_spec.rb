require 'rspec'

module CassandraModel
  describe TableDescriptor do
    let(:connection) { double(:connection) }

    subject { TableDescriptor.new({}) }

    before { allow(TableDescriptor).to receive(:connection).and_return(connection) }

    it { is_expected.to be_a_kind_of(Record) }

    describe '.create_descriptor_table' do
      it 'should create the table in cassandra' do
        expected_query = 'CREATE TABLE table_descriptors (name ascii, created_at timestamp, id ascii, PRIMARY KEY ((name), created_at, id))'
        expect(connection).to receive(:execute).with(expected_query)
        TableDescriptor.create_descriptor_table
      end
    end

    describe '.drop_descriptor_table' do
      it 'should drop the table from cassandra' do
        expected_query = 'DROP TABLE table_descriptors'
        expect(connection).to receive(:execute).with(expected_query)
        TableDescriptor.drop_descriptor_table
      end
    end
  end
end