require 'rspec'

module CassandraModel
  describe DisplayableAttributes do
    let(:attributes) { {} }
    let(:field_type) { :text }
    let(:cassandra_columns) { {'partition' => :text, 'clustering' => :text, 'some' => field_type} }

    subject { Record.new(attributes, validate: false) }

    before do
      mock_simple_table(:records, [], [], [])
      allow(Record).to receive(:cassandra_columns).and_return(cassandra_columns)
      allow(Record).to receive(:select_column) { |column| column }
    end
    after { Record.reset! }

    describe '#as_json' do
      its(:as_json) { is_expected.to eq(attributes) }

      it 'should support taking in a parameter' do
        expect { subject.as_json(some: :options) }.not_to raise_error
      end

      context 'with different attributes' do
        let(:attributes) { {partition: 'Key', clustering: 'Columns', some: 'Field'} }

        its(:as_json) { is_expected.to eq(attributes) }
      end

      context 'when a column provided is a blob' do
        let(:field_type) { :blob }
        let(:attributes) { {partition: 'Key', clustering: 'Columns', some: 'Field'} }

        its(:as_json) { is_expected.to eq(attributes.except(:some)) }
      end

      context 'when a column provided is a Cassandra::Uuid' do
        let(:field_type) { :uuid }
        let(:some_uuid) { SecureRandom.uuid }
        let(:attributes) { {partition: 'Key', clustering: 'Columns', some: Cassandra::Uuid.new(some_uuid)} }

        its(:as_json) { is_expected.to eq(attributes.merge(some: some_uuid)) }

        context 'when the column is mapped' do
          let(:attributes) { {some: Cassandra::Uuid.new(some_uuid)} }
          let(:cassandra_columns) { {'rk_some' => field_type} }

          before { allow(Record).to receive(:select_column) { |column| :"rk_#{column}" } }

          its(:as_json) { is_expected.to eq(some: some_uuid) }
        end
      end

      context 'with deferred columns' do
        let(:attributes) { {partition: 'Key'} }
        before do
          Record.deferred_column :fake_column, on_load: ->(attributes) { 'fake data' }
        end
        after { Record.send(:remove_method, :fake_column) if Record.instance_methods(false).include?(:fake_column) }

        its(:as_json) { is_expected.to eq(partition: 'Key', fake_column: 'fake data') }
      end

      context 'when configured to only return certain columns' do
        let(:attributes) { {partition: 'Key', clustering: 'Columns', some: 'Field'} }
        let(:display_columns) { [:partition] }

        before { Record.display_attributes(*display_columns) }

        its(:as_json) { is_expected.to eq(partition: 'Key') }

        context 'with a different slice' do
          let(:display_columns) { [:clustering, :some] }

          its(:as_json) { is_expected.to eq(clustering: 'Columns', some: 'Field') }
        end

        context 'when the display attributes represent a column name map' do
          let(:display_columns) { [{partition: 'Partition Key'}] }

          its(:as_json) { is_expected.to eq('Partition Key' => 'Key') }

          context 'with a different map' do
            let(:display_columns) { [{partition: 'Part', some: 'Description'}] }

            its(:as_json) { is_expected.to eq('Part' => 'Key', 'Description' => 'Field') }
          end
        end
      end
    end
  end
end
