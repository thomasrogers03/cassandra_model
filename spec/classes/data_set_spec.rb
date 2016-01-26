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
          subject.knows_about(:name, :apartment_at)
          expect(subject.columns).to eq(name: :text, apartment_at: :text)
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

    it_behaves_like 'a data set guessing a type', :columns, :counter

    describe '#counts' do
      it 'should define a counter column' do
        subject.counts
        expect(subject.columns).to eq(count: :counter)
      end

      context 'with multiple, named counters' do
        let(:columns) { [:failed, :completed, :pending] }

        it 'should define counter columns with the specified names' do
          subject.counts(*columns)
          expect(subject.columns).to eq(failed: :counter, completed: :counter, pending: :counter)
        end
      end
    end

    describe '#clustering_columns' do
      it 'should be an empty array by default' do
        expect(subject.clustering_columns).to eq([])
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

      context 'when called multiple time' do
        let(:first_set) { Faker::Lorem.words.map(&:to_sym) }
        let(:second_set) { Faker::Lorem.words.map(&:to_sym) }

        it 'should be defined by all of those columns' do
          subject.is_defined_by(*first_set)
          subject.is_defined_by(*second_set)
          expect(subject.clustering_columns).to eq(first_set + second_set)
        end
      end
    end

    describe '#retype' do
      let(:column) { :series }

      context 'when the column is known' do
        before { subject.knows_about(column) }

        it 'should change the column type' do
          subject.change_type_of(:series).to(:int)
          expect(subject.columns).to eq(series: :int)
        end

        context 'with a different column and default value' do
          let(:column) { :created_at }

          it 'should default the specified column to the requested value' do
            subject.change_type_of(:created_at).to(:timestamp)
            expect(subject.columns).to eq(created_at: :timestamp)
          end
        end
      end

      context 'when the column is not known' do
        it 'should raise an error' do
          expect { subject.change_type_of(:series) }.to raise_error("Cannot retype unknown column #{column}")
        end

        context 'with a different column' do
          let(:column) { :created_at }

          it 'should raise an error' do
            expect { subject.change_type_of(:created_at) }.to raise_error("Cannot retype unknown column #{column}")
          end
        end
      end
    end

  end
end
