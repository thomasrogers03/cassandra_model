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

    describe '#retype' do
      let(:column) { :series }

      context 'when the column is known' do
        before { subject.knows_about(column) }

        it 'should change the column type' do
          subject.retype(:series).to(:int)
          expect(subject.columns).to eq(series: :int)
        end

        context 'with a different column and default value' do
          let(:column) { :created_at }

          it 'should default the specified column to the requested value' do
            subject.retype(:created_at).to(:timestamp)
            expect(subject.columns).to eq(created_at: :timestamp)
          end
        end
      end

      context 'when the column is not known' do
        it 'should raise an error' do
          expect { subject.retype(:series) }.to raise_error("Cannot retype unknown column #{column}")
        end

        context 'with a different column' do
          let(:column) { :created_at }

          it 'should raise an error' do
            expect { subject.retype(:created_at) }.to raise_error("Cannot retype unknown column #{column}")
          end
        end
      end
    end

  end
end