require 'rspec'

module CassandraModel
  describe DataSet do

    describe '#knows_about' do
      it 'should define the columns of data within the set' do
        subject.knows_about(:title, :series)
        expect(subject.columns).to eq(title: :string, series: :string)
      end

      context 'with different columns' do
        it 'should define the columns of data within the set' do
          subject.knows_about(:name, :apartment)
          expect(subject.columns).to eq(name: :string, apartment: :string)
        end
      end

      context 'when called multiple time' do
        it 'should define a unique set of columns' do
          subject.knows_about(:title, :name)
          subject.knows_about(:name, :series)
          expect(subject.columns).to eq(title: :string, name: :string, series: :string)
        end
      end
    end

    describe '#is_defined_by' do
      it 'should define the clustering columns for this data set' do
        subject.is_defined_by(:name)
        expect(subject.clustering_columns).to eq([:name])
      end

      it 'should "know" about the columns' do
        subject.is_defined_by(:name)
        expect(subject.columns).to eq(name: :string)
      end

      context 'with different columns' do
        it 'should define the clustering columns for this data set' do
          subject.is_defined_by(:title, :series)
          expect(subject.clustering_columns).to eq([:title, :series])
        end

        it 'should "know" about the columns' do
          subject.is_defined_by(:title, :series)
          expect(subject.columns).to eq(title: :string, series: :string)
        end
      end

    end

  end
end