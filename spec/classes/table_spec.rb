require 'rspec'

module CassandraModel
  describe Table do
    let(:table_name) { :records }
    let(:klass) { Table }

    subject { klass.new(table_name) }

    before do
      mock_simple_table(table_name, [:partition], [:cluster], [:misc])
      klass.reset!
    end

    it_behaves_like 'a model with a connection', Table
    it_behaves_like 'a table'

    describe '#name' do
      its(:name) { is_expected.to eq('records') }
    end

  end
end