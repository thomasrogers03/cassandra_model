require 'rspec'

module CassandraModel
  describe DisplayableAttributes do
    let(:attributes) { {} }

    subject { Record.new(attributes, validate: false) }

    before { mock_simple_table(:records, [], [], []) }
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
