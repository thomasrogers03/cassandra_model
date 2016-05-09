require 'rspec'

module CassandraModel
  describe TableDefinition do
    let(:partition_key) { {title: :text} }
    let(:clustering_columns) { {series: :int} }
    let(:remaining_columns) { {body: :text} }
    let(:table_name) { :books }
    let(:properties) { nil }
    let(:options) do
      {
          name: table_name,
          partition_key: partition_key,
          clustering_columns: clustering_columns,
          remaining_columns: remaining_columns,
          properties: properties
      }
    end
    let(:definition) { TableDefinition.new(options) }

    subject { definition }

    its(:name) { is_expected.to eq(table_name) }

    its(:name_in_cassandra) { is_expected.to eq("#{subject.name}_#{subject.table_id}") }

    describe '.from_data_model' do
      let(:inquirer) { DataInquirer.new }
      let(:data_set) { DataSet.new }
      let(:partition_key) { {title: :string, series: :string, year: :int} }
      let(:rk_partition_key) do
        partition_key.inject({}) do |memo, (key, value)|
          memo.merge!(:"rk_#{key}" => value)
        end
      end
      let(:clustering_columns) { {price: :double} }
      let(:ck_clustering_columns) do
        clustering_columns.inject({}) do |memo, (key, value)|
          memo.merge!(:"ck_#{key}" => value)
        end
      end
      let(:remaining_columns) { {description: :string} }
      let(:attributes) do
        {
            name: table_name,
            partition_key: rk_partition_key,
            clustering_columns: ck_clustering_columns,
            remaining_columns: remaining_columns,
            properties: properties
        }
      end

      subject { TableDefinition.from_data_model(table_name, inquirer, data_set, properties) }

      before do
        inquirer.knows_about(*partition_key.keys)
        partition_key.each { |key, value| inquirer.change_type_of(key).to(value) }

        data_set.is_defined_by(*clustering_columns.keys)
        clustering_columns.each { |key, value| data_set.change_type_of(key).to(value) }

        data_set.knows_about(*remaining_columns.keys)
        remaining_columns.each { |key, value| data_set.change_type_of(key).to(value) }
      end

      it 'should generate a table definition from an inquirer/data set pair' do
        is_expected.to eq(TableDefinition.new(attributes))
      end

      context 'with a different table' do
        let(:table_name) { :images }
        let(:partition_key) { {make: :string, model: :string, year: :int} }
        let(:clustering_columns) { {model: :string, price: :double} }
        let(:remaining_columns) { {} }

        it 'should generate a table definition from an inquirer/data set pair' do
          is_expected.to eq(TableDefinition.new(attributes))
        end
      end

      context 'when the inquirer shards requests' do
        let(:sharding_column_name) { Faker::Lorem.word.to_sym }
        let(:sharding_column) { sharding_column_name }
        let(:sharding_definition) { {:"rk_#{sharding_column_name}" => :int} }
        let(:rk_partition_key) do
          partition_key.inject({}) do |memo, (key, value)|
            memo.merge!(:"rk_#{key}" => value)
          end.merge(sharding_definition)
        end

        before { inquirer.shards_queries(sharding_column) }

        it 'should generate a table definition from an inquirer/data set pair' do
          is_expected.to eq(TableDefinition.new(attributes))
        end

        context 'when the shard is a hash' do
          let(:sharding_column) { {sharding_column_name => :double} }
          let(:sharding_definition) { {:"rk_#{sharding_column_name}" => :double} }

          it 'should override the type' do
            is_expected.to eq(TableDefinition.new(attributes))
          end
        end
      end

      context 'with properties' do
        let(:properties) { {clustering_order: {value: :desc}, compaction: {class: 'LeveledCompactionStrategy'}} }

        it 'should override the type' do
          is_expected.to eq(TableDefinition.new(attributes))
        end
      end
    end

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

      context 'with no clustering columns' do
        let(:clustering_columns) { {} }

        it { is_expected.to eq("CREATE TABLE #{definition.name_in_cassandra} (title text, body text, PRIMARY KEY ((title)))") }
      end

      context 'when requested to ignore the table id' do
        subject { TableDefinition.new(options).to_cql(no_id: true) }

        it { is_expected.to eq('CREATE TABLE books (title text, series int, body text, PRIMARY KEY ((title), series))') }
      end

      context 'when we want to check whether the table already exists' do
        subject { TableDefinition.new(options).to_cql(check_exists: true) }

        it { is_expected.to eq("CREATE TABLE IF NOT EXISTS #{definition.name_in_cassandra} (title text, series int, body text, PRIMARY KEY ((title), series))") }
      end

      describe 'table properties' do
        let(:partition_key) { {key: :text} }
        let(:clustering_columns) { {value: :text} }
        let(:remaining_columns) { {} }

        describe 'compaction strategies' do
          let(:properties) { {compaction: {class: 'DateTieredCompactionStrategy', base_time_seconds: '3600', max_sstable_age_days: '7'}} }
          it { is_expected.to eq("CREATE TABLE #{definition.name_in_cassandra} (key text, value text, PRIMARY KEY ((key), value)) WITH COMPACTION = {'class': 'DateTieredCompactionStrategy', 'base_time_seconds': '3600', 'max_sstable_age_days': '7'}") }

          context 'with a different compaction strategy configured' do
            let(:properties) { {compaction: {class: 'LeveledCompactionStrategy'}} }
            it { is_expected.to eq("CREATE TABLE #{definition.name_in_cassandra} (key text, value text, PRIMARY KEY ((key), value)) WITH COMPACTION = {'class': 'LeveledCompactionStrategy'}") }
          end
        end

        describe 'clustering order' do
          let(:properties) { {clustering_order: {value: :desc}} }
          it { is_expected.to eq("CREATE TABLE #{definition.name_in_cassandra} (key text, value text, PRIMARY KEY ((key), value)) WITH CLUSTERING ORDER BY (value DESC)") }

          context 'with a different clustering order' do
            let(:clustering_columns) { {value: :text, other_value: :text} }
            let(:properties) { {clustering_order: {value: :asc, other_value: :desc}} }
            it { is_expected.to eq("CREATE TABLE #{definition.name_in_cassandra} (key text, value text, other_value text, PRIMARY KEY ((key), value, other_value)) WITH CLUSTERING ORDER BY (value ASC, other_value DESC)") }
          end
        end

        context 'with multiple properties' do
          let(:properties) { {clustering_order: {value: :desc}, compaction: {class: 'LeveledCompactionStrategy'}} }
          it { is_expected.to eq("CREATE TABLE #{definition.name_in_cassandra} (key text, value text, PRIMARY KEY ((key), value)) WITH CLUSTERING ORDER BY (value DESC) AND COMPACTION = {'class': 'LeveledCompactionStrategy'}") }
        end
      end
    end

  end
end
