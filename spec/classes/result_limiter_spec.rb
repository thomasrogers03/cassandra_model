require 'spec_helper'

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
        it 'returns itself' do
          expect(result_limiter.each).to eq(result_limiter)
        end
      end
    end

    describe '#get' do
      let(:enum) { Faker::Lorem.words }
      subject { result_limiter.get }
      it { is_expected.to eq(result_limiter.to_a) }
    end

    describe '#==' do
      let(:enum) { Faker::Lorem.words }
      let(:limit) { 15 }
      let(:enum_two) { enum }
      let(:limit_two) { limit }
      let(:result_limiter_two) { ResultLimiter.new(enum_two, limit_two) }

      subject { result_limiter == result_limiter_two }

      it { is_expected.to eq(true) }

      context 'with a different enum' do
        let(:enum_two) { Faker::Lorem.words }
        it { is_expected.to eq(false) }
      end

      context 'with a different limit' do
        let(:limit_two) { 71 }
        it { is_expected.to eq(false) }
      end

      context 'when not a limiter' do
        let(:result_limiter_two) { [] }
        it { is_expected.to eq(false) }
      end
    end

  end
end
