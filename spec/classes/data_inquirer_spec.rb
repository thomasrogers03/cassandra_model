require 'rspec'

module CassandraModel
  describe DataInquirer do

    describe '#knows_about' do
      it 'should return itself' do
        expect(subject.knows_about).to eq(subject)
      end

      it 'should define the partition key based on what the inquirer knows about' do
        subject.knows_about(:name)
        expect(subject.partition_key).to eq(name: :string)
      end

      context 'with different columns' do
        it 'should define the partition key' do
          subject.knows_about(:title, :series)
          expect(subject.partition_key).to eq(title: :string, series: :string)
        end
      end
    end

  end
end