require 'rspec'

module CassandraModel
  describe TableDefinition do
    let(:partition_key) { [:title] }
    let(:clustering_columns) { [:series] }
    let(:columns) { {title: :text, series: :int, body: :text} }
    let(:table_name) { :books }
    let(:options) { {name: table_name, columns: columns, partition_key: partition_key, clustering_columns: clustering_columns} }

    describe '#to_cql' do
      subject { TableDefinition.new(options).to_cql }

      it { is_expected.to eq('CREATE TABLE books (title text, series int, body text, PRIMARY KEY ((title), series))') }

      context 'with a different table name' do
        let(:table_name) { :movies }

        it { is_expected.to eq('CREATE TABLE movies (title text, series int, body text, PRIMARY KEY ((title), series))') }
      end

      context 'with different columns' do
        let(:columns) { {title: :text, series: :int, isbn: :text, location: :int} }

        it { is_expected.to eq('CREATE TABLE books (title text, series int, isbn text, location int, PRIMARY KEY ((title), series))') }
      end

      context 'with a different partition key' do
        let(:partition_key) { [:name, :subtitle] }
        let(:columns) { {name: :text, subtitle: :text, series: :int, body: :text} }

        it { is_expected.to eq('CREATE TABLE books (name text, subtitle text, series int, body text, PRIMARY KEY ((name, subtitle), series))') }
      end

      context 'with different clustering columns' do
        let(:clustering_columns) { [:series, :episode] }
        let(:columns) { {title: :text, series: :int, episode: :int, body: :text} }

        it { is_expected.to eq('CREATE TABLE books (title text, series int, episode int, body text, PRIMARY KEY ((title), series, episode))') }
      end
    end
  end
end