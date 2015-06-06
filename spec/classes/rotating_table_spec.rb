require 'rspec'

module CassandraModel
  describe RotatingTable do
    let(:partition_key) { [:partition_key] }
    let(:clustering_columns) { [:clustering_key] }
    let(:remaining_columns) { [:meta_data] }
    let(:columns) { partition_key + clustering_columns + remaining_columns }
    let(:table_methods) do
      {partition_key: partition_key,
       clustering_columns: clustering_columns,
       columns: columns}
    end
    let(:first_table) { double(:table, table_methods.merge(name: 'table 1')) }
    let(:second_table) { double(:table, table_methods.merge(name: 'table 2')) }
    let(:third_table) { double(:table, table_methods.merge(name: 'table 3')) }
    let(:rotating_schedule) { 1.week }
    let(:rotating_table) { RotatingTable.new([first_table, second_table, third_table], rotating_schedule) }

    subject { rotating_table }

    describe 'validation' do
      describe 'column validation' do
        let(:second_table_columns) { [:different_partition, :description] }

        before do
          allow(second_table).to receive(:columns).and_return(second_table_columns)
        end

        it 'should raise an error when the columns of each table do not match' do
          expect { subject }.to raise_error('RotatingTable, Table columns do not match')
        end
      end
    end

    describe '#name' do
      let(:base_time) { Time.at(0) }
      let(:time) { base_time }

      around do |example|
        Timecop.freeze(time) { example.run }
      end

      subject { rotating_table.name }

      it 'should use the initial table' do
        is_expected.to eq('table 1')
      end

      context 'when rotating to the second week' do
        let(:time) { base_time + 1.week }

        it { is_expected.to eq('table 2') }
      end

      context 'when rotating to the third week' do
        let(:time) { base_time + 2.weeks }

        it { is_expected.to eq('table 3') }
      end

      context 'when rotating in between weeks' do
        let(:time) { base_time + 1.week + 3.days }

        it { is_expected.to eq('table 2') }
      end

      context 'when specifying an alternate rotating schedule' do
        let(:rotating_schedule) { 3.days }

        it 'should use the initial table' do
          is_expected.to eq('table 1')
        end

        context 'when rotating to the second week' do
          let(:time) { base_time + 3.days }

          it { is_expected.to eq('table 2') }
        end

        context 'when rotating to the third week' do
          let(:time) { base_time + 6.days }

          it { is_expected.to eq('table 3') }
        end
      end
    end
  end
end