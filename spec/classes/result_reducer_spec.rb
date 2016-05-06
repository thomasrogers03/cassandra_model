require 'rspec'

module CassandraModel
  describe ResultReducer do

    let(:record_klass) { Struct.new(:attributes) }
    let(:enum) { [] }
    let(:filter_keys) { [] }
    let(:reducer) { ResultReducer.new(enum, filter_keys) }

    subject { reducer }

    it { is_expected.to be_a_kind_of(Enumerable) }

    describe '#each' do
      let(:results) { [] }

      subject { results }

      before { reducer.each { |value| results << value } }

      it { is_expected.to eq([]) }

      context 'with some results' do
        let(:key) { Faker::Lorem.word }
        let(:value) { Faker::Lorem.word }
        let(:attributes) { {key: key, value: value} }
        let(:row) { record_klass.new(attributes) }
        let(:enum) { [[0, [row]]] }

        it { is_expected.to eq([row]) }

        context 'with a filter' do
          let(:filter_keys) { [:key] }

          context 'with multiple results' do
            let(:key_two) { key }
            let(:value_two) { Faker::Lorem.sentence }
            let(:attributes_two) { {key: key_two, value: value_two} }
            let(:row_two) { record_klass.new(attributes_two) }
            let(:enum) { [[0, [row, row_two]]] }

            it { is_expected.to eq([row]) }

            context 'with a longer filter' do
              let(:filter_keys) { [:key, :value] }
              it { is_expected.to eq([row, row_two]) }

              context 'with even more results' do
                let(:value_two) { value }

                let(:key_three) { key }
                let(:value_three) { Faker::Lorem.sentence }
                let(:attributes_three) { {key: key_three, value: value_three} }
                let(:row_three) { record_klass.new(attributes_three) }
                let(:enum) { [[0, [row, row_two, row_three]]] }

                it { is_expected.to eq([row, row_two]) }

                context 'with an even longer filter' do
                  let(:value_three) { value }
                  let(:description) { Faker::Lorem.sentence }
                  let(:description_two) { Faker::Lorem.sentence }
                  let(:description_three) { Faker::Lorem.sentence }
                  let(:attributes) do
                    {key: key, value: value, description: description}
                  end
                  let(:attributes_two) do
                    {key: key_two, value: value_two, description: description_two}
                  end
                  let(:attributes_three) do
                    {key: key_three, value: value_three, description: description_three}
                  end
                  let(:filter_keys) { [:key, :value, :description] }

                  it { is_expected.to eq([row, row_two, row_three]) }
                end
              end
            end
          end

        end
      end
    end

  end
end
