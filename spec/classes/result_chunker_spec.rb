require 'rspec'

module CassandraModel
  describe ResultChunker do

    let(:model_klass) { Struct.new(:attributes) }
    let(:cluster) { [] }
    let(:enum) { [] }
    let(:chunker) { ResultChunker.new(enum, cluster) }

    subject { chunker }

    it { is_expected.to be_a_kind_of(Enumerable) }

    describe '#each' do
      let(:results) { [] }

      subject { results }

      before { chunker.each { |cluster, rows| results << [cluster, rows] } }

      it { is_expected.to be_empty }

      context 'with results' do
        let(:attributes) { {key: :value} }
        let(:row) { model_klass.new(attributes) }
        let(:enum) { [row] }

        it { is_expected.to eq([[[], [row]]]) }

        context 'with multiple results' do
          let(:attributes_two) { {key: :value_two} }
          let(:row_two) { model_klass.new(attributes_two) }
          let(:enum) { [row, row_two] }

          it { is_expected.to eq([[[], [row, row_two]]]) }

          context 'with a cluster provided' do
            let(:cluster) { [:key] }

            it { is_expected.to eq([[[:value], [row]], [[:value_two], [row_two]]]) }

            context 'with more attributes' do
              let(:cluster) { [:key, :key_two] }
              let(:attributes) { {key: :value, key_two: :value} }
              let(:attributes_two) { {key: :value, key_two: :value_two} }

              it { is_expected.to eq([[[:value, :value], [row]], [[:value, :value_two], [row_two]]]) }
            end
          end
        end
      end
    end

  end
end
