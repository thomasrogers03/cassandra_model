require 'rspec'

module CassandraModel
  describe ResultReducerByKeys do

    let(:columns) { [:hello] }
    let(:record_klass) { Struct.new(*columns) }
    let(:keys) { [] }
    let(:enum) { [] }
    let(:reducer) { ResultReducerByKeys.new(enum, keys) }

    subject { reducer }

    it { is_expected.to be_a_kind_of(Enumerable) }

    describe '#each' do
      describe 'the results' do
        let(:results) { [] }

        before { reducer.each { |value| results << value } }

        subject { results }

        it { is_expected.to eq([]) }

        context 'with results' do
          let(:enum) { rand(1..5).times.map { record_klass.new(Faker::Lorem.word) } }

          it { is_expected.to eq(enum) }

          context 'with filter keys' do
            let(:enum) { [record_klass.new('hello'), record_klass.new('hello')] }
            let(:keys) { [:hello] }

            it { is_expected.to eq([record_klass.new('hello')]) }

            context 'with a different key column' do
              let(:columns) { [:hello, :world] }
              let(:keys) { [:world] }
              let(:enum) { [record_klass.new('hello', 'world'), record_klass.new('hello', 'bob')] }

              it { is_expected.to eq(enum) }
            end
          end
        end

        describe 'the enumerator' do
          let(:enum) { [record_klass.new('hello'), record_klass.new('hello')] }
          let(:keys) { [:hello] }

          subject { reducer.each.to_a }

          it { is_expected.to eq([record_klass.new('hello')]) }
        end

      end
    end

  end
end
