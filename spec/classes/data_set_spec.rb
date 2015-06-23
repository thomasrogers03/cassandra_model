require 'rspec'

module CassandraModel
  describe DataSet do

    describe '#knows_about' do
      it 'should define the columns of data within the set' do
        subject.knows_about(:title, :series)
        expect(subject.columns).to eq(title: :text, series: :text)
      end

      context 'with different columns' do
        it 'should define the columns of data within the set' do
          subject.knows_about(:name, :apartment)
          expect(subject.columns).to eq(name: :text, apartment: :text)
        end
      end

      context 'when called multiple time' do
        it 'should define a unique set of columns' do
          subject.knows_about(:title, :name)
          subject.knows_about(:name, :series)
          expect(subject.columns).to eq(title: :text, name: :text, series: :text)
        end
      end
    end

    describe '#rotates_storage_across' do
      it 'should indicate that this data set will rotate the tables it is stored in' do
        subject.rotates_storage_across(5).tables
        expect(subject.data_rotation).to eq(slices: 5, frequency: 1.week)
      end

      context 'with a different number of table slices' do
        it 'should use the specified number of tables to rotate' do
          subject.rotates_storage_across(15).tables
          expect(subject.data_rotation).to eq(slices: 15, frequency: 1.week)
        end
      end

      context 'when specifying a frequency' do
        it 'should the frequency to determine the rotation schedule' do
          subject.rotates_storage_across(10).tables_every(20.minutes)
          expect(subject.data_rotation).to eq(slices: 10, frequency: 20.minutes)
        end

        context 'with a different frequency' do
          it 'should use the specified frequency' do
            subject.rotates_storage_across(10).tables_every(5.hours)
            expect(subject.data_rotation).to eq(slices: 10, frequency: 5.hours)
          end
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
        expect(subject.columns).to eq(name: :text)
      end

      context 'with different columns' do
        it 'should define the clustering columns for this data set' do
          subject.is_defined_by(:title, :series)
          expect(subject.clustering_columns).to eq([:title, :series])
        end

        it 'should "know" about the columns' do
          subject.is_defined_by(:title, :series)
          expect(subject.columns).to eq(title: :text, series: :text)
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