require 'rspec'

module CassandraModel
  describe DisplayableAttributes do
    let(:attributes) { {} }

    subject { Record.new(attributes, validate: false) }

    before { mock_simple_table(:records, [], [], []) }

    describe '#as_json' do
      its(:as_json) { is_expected.to eq(attributes) }

      context 'with different attributes' do
        let(:attributes) { {partition: 'Key', clustering: 'Columns', some: 'Field'} }

        its(:as_json) { is_expected.to eq(attributes) }
      end

      it 'should support taking in a parameter' do
        expect { subject.as_json(some: :options) }.not_to raise_error
      end
    end
  end
end
