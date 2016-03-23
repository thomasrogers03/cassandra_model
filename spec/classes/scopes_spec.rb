require 'rspec'

module CassandraModel
  describe Scopes do

    let(:klass) do
      Class.new(Record) do
        extend Scopes

        def self.name
          Faker::Lorem.word
        end
      end
    end

    describe '.scope' do
      let(:key) { Faker::Lorem.word }
      let(:value) { Faker::Lorem.sentence }
      let(:scope_name) { Faker::Lorem.word.to_sym }
      let(:scope) do
        scope_key = key
        scope_value = value
        ->() { where(scope_key => scope_value) }
      end
      let(:scope_args) { [] }

      before { klass.scope(scope_name, scope) }

      subject { klass.public_send(scope_name, *scope_args) }

      it { is_expected.to eq(klass.where(key => value)) }

      it 'should store the scope' do
        subject
        expect(klass.scopes).to include(scope_name => scope)
      end

      context 'with a scope taking parameters' do
        let(:scope_args) { [key, value] }
        let(:scope) { ->(scope_key, scope_value) { where(scope_key => scope_value) } }

        it { is_expected.to eq(klass.where(key => value)) }
      end
    end

  end
end
