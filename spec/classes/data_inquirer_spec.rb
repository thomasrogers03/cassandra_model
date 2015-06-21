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

      it 'should define default values for the specified columns' do
        subject.knows_about(:name)
        expect(subject.column_defaults).to eq(name: '')
      end

      context 'with different columns' do
        before { subject.knows_about(:title, :series) }

        it 'should define the partition key' do
          expect(subject.partition_key).to eq(title: :string, series: :string)
        end

        it 'should define default values for the specified columns' do
          expect(subject.column_defaults).to eq(title: '', series: '')
        end
      end

      context 'when called multiple times' do
        it 'should define multiple rows of keys for composite defaults' do
          subject.knows_about(:name)
          subject.knows_about(:title, :series)
          expect(subject.composite_rows).to eq([[:title, :series], [:name]])
        end

        context 'with different columns' do
          it 'should define multiple rows of keys for composite defaults' do
            subject.knows_about(:title, :series)
            subject.knows_about(:author, :title)
            expect(subject.composite_rows).to eq([[:author], [:series]])
          end
        end
      end
    end

    describe '#defaults' do
      let(:column) { :title }

      context 'when the column is known' do
        before { subject.knows_about(column) }

        it 'should default the specified column to the requested value' do
          subject.defaults(:title).to('NULL')
          expect(subject.column_defaults).to eq(title: 'NULL')
        end

        context 'with a different colun and default value' do
          let(:column) { :series }

          it 'should default the specified column to the requested value' do
            subject.defaults(:series).to('1A')
            expect(subject.column_defaults).to eq(series: '1A')
          end
        end
      end

      context 'when the column is not known' do
        it 'should raise an error' do
          expect { subject.defaults(:title) }.to raise_error("Cannot default unknown column #{column}")
        end

        context 'with a different colun and default value' do
          let(:column) { :series }

          it 'should raise an error' do
            expect { subject.defaults(:series) }.to raise_error("Cannot default unknown column #{column}")
          end
        end
      end

    end

    describe 'typing' do

      context 'when mixing #knows_about and #defaults' do
        it 'should infer the type from the default value' do
          subject.knows_about(:series).defaults(:series).to(0)
          expect(subject.partition_key).to eq(series: :int)
        end

        context 'with a different type' do
          it 'should infer the type from the default value' do
            subject.knows_about(:price).defaults(:price).to(0.0)
            expect(subject.partition_key).to eq(price: :double)
          end
        end
      end

    end

  end
end