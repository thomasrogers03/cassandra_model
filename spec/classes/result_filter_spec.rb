require 'rspec'

module CassandraModel
  describe ResultFilter do

    let(:enum) { [] }
    let(:filter_block) { ->(_) { true } }
    let(:filter) { ResultFilter.new(enum, &filter_block) }

    subject { filter }

    it { is_expected.to be_a_kind_of(Enumerable) }

    describe '#each' do
      let(:results) { [] }

      before { filter.each { |value| results << value } }

      describe 'the results' do
        subject { results }

        it { is_expected.to eq([]) }

        context 'with items' do
          let(:enum) { Faker::Lorem.words }

          it { is_expected.to eq(enum) }

          context 'with a filter' do
            let(:filter_list) { enum.sample(2) }
            let(:filter_block) { ->(value) { !filter_list.include?(value) } }

            it { is_expected.to eq(enum - filter_list) }
          end
        end
      end

      describe 'the enumerator' do
        let(:enum) { Faker::Lorem.words }
        let(:filter_list) { enum.sample(2) }
        let(:filter_block) { ->(value) { !filter_list.include?(value) } }

        subject { filter.each }

        it { is_expected.to be_a_kind_of(Enumerator) }

        its(:to_a) { is_expected.to eq(enum - filter_list) }
      end

    end


  end
end
