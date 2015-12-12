module CassandraModel
  shared_examples_for 'a data set guessing a type' do |column_set_name, counter_type|
    describe '#guess_data_types!' do
      let(:column_name) { :title }

      before do
        subject.guess_data_types!
        subject.knows_about(column_name)
      end

      its(column_set_name) { is_expected.to eq(title: :text) }

      shared_examples_for 'a data type determine by a postfix' do |postfix, type|
        context "when the column name ends in _#{postfix}" do
          let(:column_name) { :"created_#{postfix}" }
          its(column_set_name) { is_expected.to eq(column_name => type) }

          context 'with a different column' do
            let(:column_name) { :"updated_#{postfix}" }
            its(column_set_name) { is_expected.to eq(column_name => type) }
          end
        end

        context "when the column is exactly #{postfix}" do
          let(:column_name) { :"#{postfix}" }

          its(column_set_name) { is_expected.to eq(column_name => type) }
        end
      end

      it_behaves_like 'a data type determine by a postfix', :at, :timestamp
      it_behaves_like 'a data type determine by a postfix', :at_id, :timeuuid
      it_behaves_like 'a data type determine by a postfix', :id, :uuid
      it_behaves_like 'a data type determine by a postfix', :price, :double
      it_behaves_like 'a data type determine by a postfix', :average, :double
      it_behaves_like 'a data type determine by a postfix', :stddev, :double
      it_behaves_like 'a data type determine by a postfix', :year, :int
      it_behaves_like 'a data type determine by a postfix', :day, :int
      it_behaves_like 'a data type determine by a postfix', :month, :int
      it_behaves_like 'a data type determine by a postfix', :index, :int
      it_behaves_like 'a data type determine by a postfix', :count, counter_type
      it_behaves_like 'a data type determine by a postfix', :total, counter_type
      it_behaves_like 'a data type determine by a postfix', :map, 'map<string, string>'
      it_behaves_like 'a data type determine by a postfix', :data, :blob
    end
  end
end
