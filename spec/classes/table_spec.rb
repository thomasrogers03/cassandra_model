require 'rspec'

module CassandraModel
  describe Table do
    let(:table_name) { :records }
    let(:cluster) { double(:cluster, connect: connection) }
    let(:connection) { double(:connection) }
    let(:column_object) { double(:column, name: 'partition') }
    let(:table_object) { double(:table, columns: [column_object]) }
    let(:keyspace) { double(:keyspace, table: table_object) }
    let(:klass) { Table }

    subject { klass.new(table_name) }

    before do
      klass.reset!
      allow(Cassandra).to receive(:cluster).and_return(cluster)
      allow(cluster).to receive(:keyspace).with(klass.config[:keyspace]).and_return(keyspace)
    end

    it_behaves_like 'a model with a connection', Table
    it_behaves_like 'a table'
  end
end