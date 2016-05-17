require 'rspec'

module CassandraModel
  describe ResultCombiner do

    let(:enum) { [] }
    let(:enum_two) { [] }
    let(:combiner) { ResultCombiner.new(enum, enum_two) }

    subject { combiner }

    it { is_expected.to be_a_kind_of(Enumerable) }

    describe '#each' do
      let(:results) { [] }

      describe 'the results' do
        before { combiner.each { |value| results << value } }

        subject { results }

        it { is_expected.to eq([]) }

        context 'when the first one has results' do
          let(:enum) { Faker::Lorem.words }
          it { is_expected.to eq(enum) }

          context 'when the second one has results' do
            let(:enum_two) { Faker::Lorem.words }
            it { is_expected.to eq(enum + enum_two) }
          end
        end
      end

      describe 'the enumerator' do
        subject { combiner.each.to_a }

        it { is_expected.to eq(results) }

        context 'when the first one has results' do
          let(:enum) { Faker::Lorem.words }
          it { is_expected.to eq(enum) }

          context 'when the second one has results' do
            let(:enum_two) { Faker::Lorem.words }
            it { is_expected.to eq(enum + enum_two) }
          end
        end
      end

    end

  end
end
