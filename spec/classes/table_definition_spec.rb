require 'rspec'

module CassandraModel
  describe TableDefinition do
    let(:partition_key) { {title: :text} }
    let(:clustering_columns) { {series: :int} }
    let(:remaining_columns) { {body: :text} }
    let(:table_name) { :books }
    let(:options) do
      {
          name: table_name,
          partition_key: partition_key,
          clustering_columns: clustering_columns,
          remaining_columns: remaining_columns
      }
    end
    let(:definition) { TableDefinition.new(options) }

    subject { definition }

    its(:name) { is_expected.to eq(table_name) }

    its(:name_in_cassandra) { is_expected.to eq("#{subject.name}_#{subject.table_id}") }

    describe '#table_id' do
      subject { TableDefinition.new(options).table_id }

      it 'should be the md5 hash of the column definition' do
        is_expected.to eq(Digest::MD5.hexdigest('title text, series int, body text'))
      end

      context 'with different columns' do
        let(:remaining_columns) { {episode: :int, body: :text} }

        it { is_expected.to eq(Digest::MD5.hexdigest('title text, series int, episode int, body text')) }
      end
    end

    describe '#to_cql' do
      subject { TableDefinition.new(options).to_cql }

      it { is_expected.to eq("CREATE TABLE #{definition.name_in_cassandra} (title text, series int, body text, PRIMARY KEY ((title), series))") }

      context 'with a different table name' do
        let(:table_name) { :movies }

        it { is_expected.to eq("CREATE TABLE #{definition.name_in_cassandra} (title text, series int, body text, PRIMARY KEY ((title), series))") }
      end

      context 'with different columns' do
        let(:remaining_columns) { {isbn: :text, location: :int} }

        it { is_expected.to eq("CREATE TABLE #{definition.name_in_cassandra} (title text, series int, isbn text, location int, PRIMARY KEY ((title), series))") }
      end

      context 'with a different partition key' do
        let(:partition_key) { {name: :text, subtitle: :text} }
        let(:remaining_columns) { {series: :int, body: :text} }

        it { is_expected.to eq("CREATE TABLE #{definition.name_in_cassandra} (name text, subtitle text, series int, body text, PRIMARY KEY ((name, subtitle), series))") }
      end

      context 'with different clustering columns' do
        let(:clustering_columns) { {series: :int, episode: :int} }
        let(:remaining_columns) { {body: :text} }

        it { is_expected.to eq("CREATE TABLE #{definition.name_in_cassandra} (title text, series int, episode int, body text, PRIMARY KEY ((title), series, episode))") }
      end

      context 'when requested to ignore the table id' do
        subject { TableDefinition.new(options).to_cql(no_id: true) }

        it { is_expected.to eq('CREATE TABLE books (title text, series int, body text, PRIMARY KEY ((title), series))') }
      end
    end

  end
end