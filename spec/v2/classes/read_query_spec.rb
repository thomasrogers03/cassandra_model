require 'v2_spec_helper'

module CassandraModel
  module V2
    describe ReadQuery do

      let(:table_name) { Faker::Lorem.word }
      let(:table_column_names) { generate_names }
      let(:table_columns) do
        table_column_names.map { |column| Cassandra::Column.new(column, :text, :asc) }
      end
      let(:table) { double(:table, name: table_name, columns: table_columns) }
      let(:select_columns) { [] }
      let(:restrict_columns) { [] }
      let(:order) { [] }
      let(:limit) { false }

      subject { ReadQuery.new(table, select_columns, restrict_columns, order, limit) }

      describe '#column_names' do
        its(:column_names) { is_expected.to eq(table_column_names) }
      end

      describe '#select_clause' do
        its(:select_clause) { is_expected.to eq("SELECT * FROM #{table_name}") }

        context 'with some select columns' do
          let(:select_columns) { generate_names }
          its(:select_clause) { is_expected.to eq("SELECT #{select_columns * ','} FROM #{table_name}") }
        end
      end

      describe '#restriction_clause' do
        its(:restriction_clause) { is_expected.to be_nil }

        context 'with some select columns' do
          let(:restrict_columns) { generate_names }
          let(:expected_restriction) do
            restrict_columns.map { |column| "#{column} = ?" } * ' AND '
          end

          its(:restriction_clause) { is_expected.to eq(" WHERE #{expected_restriction}") }

          context 'when the key is an array' do
            let(:range_restrict_columns) { generate_names }
            let(:restrict_columns) { [range_restrict_columns] }
            let(:expected_restriction) do
              "(#{range_restrict_columns * ','}) IN (#{%w(?) * range_restrict_columns.count * ','})"
            end

            its(:restriction_clause) { is_expected.to eq(" WHERE #{expected_restriction}") }
          end

          context 'when the restriction contains non-equal comparisons' do
            let(:restrict_columns) { [:column.gt] }

            its(:restriction_clause) { is_expected.to eq(' WHERE column > ?') }

            context 'when the key is an array' do
              let(:range_restrict_columns) { generate_names }
              let(:restrict_columns) { [range_restrict_columns.le] }
              let(:expected_restriction) do
                "(#{range_restrict_columns * ','}) <= (#{%w(?) * range_restrict_columns.count * ','})"
              end

              its(:restriction_clause) { is_expected.to eq(" WHERE #{expected_restriction}") }
            end
          end
        end
      end

      describe '#ordering_clause' do
        its(:ordering_clause) { is_expected.to be_nil }

        context 'with some ordering columns' do
          let(:order) { generate_names }

          its(:ordering_clause) { is_expected.to eq(" ORDER BY #{order * ','}") }
        end
      end

      describe '#limit_clause' do
        its(:limit_clause) { is_expected.to be_nil }

        context 'with limitting enabled' do
          let(:limit) { true }

          its(:limit_clause) { is_expected.to eq(' LIMIT ?') }
        end
      end

      describe 'a query with full options' do
        let(:select_columns) { generate_names }
        let(:restrict_columns) { generate_names }
        let(:order) { generate_names }
        let(:limit) { true }

        describe '#hash' do
          let(:expected_hash) do
            [table_name, select_columns, restrict_columns, order, limit].map(&:hash).reduce(&:+)
          end

          its(:hash) { is_expected.to eq(expected_hash) }
        end

        describe '#eql?' do
          let(:select_columns_two) { select_columns }
          let(:restrict_columns_two) { restrict_columns }
          let(:order_two) { order }
          let(:limit_two) { limit }
          let(:table_name_two) { table_name }
          let(:table_two) { double(:table, name: table_name_two, columns: table_columns) }
          let(:rhs) { ReadQuery.new(table_two, select_columns_two, restrict_columns_two, order_two, limit_two) }

          it { is_expected.to be_eql(rhs) }

          context 'with a different table' do
            let(:table_name_two) { Faker::Lorem.sentence }
            it { is_expected.not_to be_eql(rhs) }
          end

          context 'with different select columns' do
            let(:select_columns_two) { generate_names }
            it { is_expected.not_to be_eql(rhs) }
          end

          context 'with different restriction columns' do
            let(:restrict_columns_two) { generate_names }
            it { is_expected.not_to be_eql(rhs) }
          end

          context 'with different ordering columns' do
            let(:order_two) { generate_names }
            it { is_expected.not_to be_eql(rhs) }
          end

          context 'with configured with a different limit option' do
            let(:limit_two) { !limit }
            it { is_expected.not_to be_eql(rhs) }
          end
        end

        describe '#to_s' do
          let(:query_to_cql) do
            [
                subject.select_clause,
                subject.restriction_clause,
                subject.ordering_clause,
                subject.limit_clause
            ] * ''
          end

          its(:to_s) { is_expected.to eq(query_to_cql) }
        end
      end

    end
  end
end
