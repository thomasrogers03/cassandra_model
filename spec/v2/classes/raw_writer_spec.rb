require 'v2_spec_helper'

module CassandraModel
  module V2
    describe RawWriter do

      let(:table_name) { Faker::Lorem.word }
      let(:partition_key) { generate_names }
      let(:clustering_columns) { generate_names }
      let(:fields) { generate_names }
      let(:column_names) { partition_key + clustering_columns + fields }
      let(:columns) do
        column_names.inject({}) do |memo, column|
          memo.merge!(column => 'text')
        end
      end
      let!(:table) { keyspace.add_table(table_name, [partition_key, *clustering_columns], columns, false) }

      subject { RawWriter.new(session, table) }

      describe '#write' do
        let(:column_values) { column_names.map { Faker::Lorem.word } }
        let(:expected_row) do
          column_names.each.with_index.inject({}) do |memo, (column, index)|
            memo.merge!(column => column_values[index])
          end
        end

        it 'returns a future' do
          expect(subject.write(column_values)).to be_a_kind_of(ThomasUtils::Observation)
        end

        it 'should write the values to the table' do
          subject.write(column_values).get
          expect(table.select('*', {}).first).to eq(expected_row)
        end
      end

    end
  end
end
