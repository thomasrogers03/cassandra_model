shared_examples_for 'an Enumerable modifier' do
  describe '#with_index' do
    let(:enum_klass) { Struct.new(:attributes) }
    let(:enum) do
      10.times.map do
        attributes = Faker::Lorem.words.inject({}) do |memo, word|
          memo.merge!(word => Faker::Lorem.sentence)
        end
        enum_klass.new(attributes)
      end
    end
    let(:result_enum) do
      index = 0
      results = []
      enum_modifier.each do |item|
        results << [item, index]
        index += 1
      end
      results
    end

    subject { enum_modifier.with_index }

    its(:to_a) { is_expected.to eq(result_enum) }
  end
end
