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

    shared_examples_for 'a table column method' do |method|
      let(:columns) { [:column1, :column2] }

      subject { rotating_table.public_send(method) }

      before { allow(first_table).to receive(method).and_return(columns) }

      it 'should match the columns of the first table' do
        is_expected.to eq(columns)
      end
    end

    describe('#partition_key') { it_behaves_like 'a table column method', :partition_key }
    describe('#clustering_columns') { it_behaves_like 'a table column method', :clustering_columns }
    describe('#columns') { it_behaves_like 'a table column method', :columns }

    shared_examples_for 'a rotating table method' do |method|
      let(:base_time) { Time.at(0) }
      let(:time) { base_time }

      before do
        allow(first_table).to receive(method).and_return('table 1 attribute')
        allow(second_table).to receive(method).and_return('table 2 attribute')
        allow(third_table).to receive(method).and_return('table 3 attribute')
      end

      around do |example|
        Timecop.freeze(time) { example.run }
      end

      subject { rotating_table.public_send(method) }

      it 'should use the initial table' do
        is_expected.to eq('table 1 attribute')
      end

      context 'when rotating to the second week' do
        let(:time) { base_time + 1.week }

        it { is_expected.to eq('table 2 attribute') }
      end

      context 'when rotating to the third week' do
        let(:time) { base_time + 2.weeks }

        it { is_expected.to eq('table 3 attribute') }
      end

      context 'when rotating in between weeks' do
        let(:time) { base_time + 1.week + 3.days }

        it { is_expected.to eq('table 2 attribute') }
      end

      context 'when specifying an alternate rotating schedule' do
        let(:rotating_schedule) { 3.days }

        it 'should use the initial table' do
          is_expected.to eq('table 1 attribute')
        end

        context 'when rotating to the second week' do
          let(:time) { base_time + 3.days }

          it { is_expected.to eq('table 2 attribute') }
        end

        context 'when rotating to the third week' do
          let(:time) { base_time + 6.days }

          it { is_expected.to eq('table 3 attribute') }
        end
      end
    end

    describe('#connection') { it_behaves_like 'a rotating table method', :connection }
    describe('#name') { it_behaves_like 'a rotating table method', :name }

    describe '#reset_local_schema!' do
      subject { rotating_table.reset_local_schema! }

      it 'should delegate to each of the tables' do
        expect(first_table).to receive(:reset_local_schema!)
        expect(second_table).to receive(:reset_local_schema!)
        expect(third_table).to receive(:reset_local_schema!)
        subject
      end
    end

  end
end