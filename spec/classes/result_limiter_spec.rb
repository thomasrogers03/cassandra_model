require 'rspec'

module CassandraModel
  describe ResultLimiter do

    let(:enum) { (0...10) }
    let(:limit) { 10 }
    let(:result_limiter) { ResultLimiter.new(enum, limit) }

    subject { result_limiter }

    it { is_expected.to be_a_kind_of(Enumerable) }

    describe '#each' do
      let(:results) { [] }

      describe 'the result' do
        subject { results }

        before { result_limiter.each { |value| results << value } }

        it { is_expected.to eq(enum.to_a) }

        context 'with a different enum' do
          let(:enum) { (10...100) }
          let(:limit) { 90 }

          it { is_expected.to eq(enum.to_a) }
        end

        context 'with a limit less than the enum' do
          let(:limit) { 5 }
          it { is_expected.to eq((0...5).to_a) }
        end

        context 'with a limit greater than the enum' do
          let(:limit) { 15 }
          it { is_expected.to eq(enum.to_a) }
        end
      end

      context 'without a block given' do
        it 'returns an Enumerator enumerating the limited result set' do
          results = result_limiter.each.map { |value| value }
          expect(results).to eq(enum.to_a)
        end
      end
    end

  end
end
