require 'rspec'

module CassandraModel
  describe DataInquirer do

    describe '#knows_about' do
      it 'should return itself' do
        expect(subject.knows_about).to eq(subject)
      end

      it 'should define the partition key based on what the inquirer knows about' do
        subject.knows_about(:name)
        expect(subject.partition_key).to eq(name: :text)
      end

      it 'should define default values for the specified columns' do
        subject.knows_about(:name)
        expect(subject.column_defaults).to eq(name: '')
      end

      it 'should not define any key rows for composite defaults when called with all known columns' do
        subject.knows_about(:title, :series)
        expect(subject.composite_rows).to eq([])
      end

      context 'with different columns' do
        before { subject.knows_about(:title, :series) }

        it 'should define the partition key' do
          expect(subject.partition_key).to eq(title: :text, series: :text)
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

    describe '#shards_queries' do
      it 'should create a sharding column at the end of the partition key' do
        subject.shards_queries
        expect(subject.partition_key).to eq(shard: :int)
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

        context 'with a different column and default value' do
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

        context 'with a different column' do
          let(:column) { :series }

          it 'should raise an error' do
            expect { subject.defaults(:series) }.to raise_error("Cannot default unknown column #{column}")
          end
        end
      end

    end

    describe '#retype' do
      let(:column) { :series }

      context 'when the column is known' do
        before { subject.knows_about(column) }

        it 'should change the column type' do
          subject.change_type_of(:series).to(:int)
          expect(subject.partition_key).to eq(series: :int)
        end

        context 'with a different column and default value' do
          let(:column) { :created_at }

          it 'should default the specified column to the requested value' do
            subject.change_type_of(:created_at).to(:timestamp)
            expect(subject.partition_key).to eq(created_at: :timestamp)
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

    describe 'typing' do

      context 'when mixing #knows_about and #defaults' do
        it 'should infer the type from the default value' do
          subject.knows_about(:series).defaults(:series).to(0)
          expect(subject.partition_key).to eq(series: :int)
        end

        context 'with a floating point type' do
          it 'should infer the type from the default value' do
            subject.knows_about(:price).defaults(:price).to(0.0)
            expect(subject.partition_key).to eq(price: :double)
          end
        end

        context 'with a timestamp' do
          it 'should infer the type from the default value' do
            subject.knows_about(:price).defaults(:price).to(Time.at(0))
            expect(subject.partition_key).to eq(price: :timestamp)
          end
        end
      end

      context 'when mixing #knows_about and #retype' do
        it 'should infer the type from the default value' do
          subject.knows_about(:series).change_type_of(:series).to(:int)
          expect(subject.column_defaults).to eq(series: 0)
        end

        context 'with a floating point type' do
          it 'should infer the type from the default value' do
            subject.knows_about(:price).change_type_of(:price).to(:double)
            expect(subject.column_defaults).to eq(price: 0.0)
          end
        end

        context 'with a timestamp' do
          it 'should infer the type from the default value' do
            subject.knows_about(:price).change_type_of(:price).to(:timestamp)
            expect(subject.column_defaults).to eq(price: Time.at(0))
          end
        end
      end

    end

  end
end