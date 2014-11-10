require 'spec_helper'

describe Record do
  class ImageData < Record

  end

  describe '.table_name' do
    it 'should be the lower-case plural of the class' do
      expect(Record.table_name).to eq('records')
    end

    context 'when inherited from a different class' do
      it { expect(ImageData.table_name).to eq('image_data') }
    end

    context 'when overridden' do
      before { Record.table_name = 'image_data' }
      it { expect(Record.table_name).to eq('image_data') }
    end
  end

  describe '.config' do
    subject { Record.config }

    let(:config) do
      {
          'host' => 'localhost',
          'keyspace' => 'default_keyspace',
          'port' => '9042'
      }
    end

    it 'should use a default configuration' do
      expect(subject).to eq(config)
    end

    context 'when specifying the options' do
      let (:config) do
        {
            'host' => 'me',
            'keyspace' => 'new_keyspace',
            'port' => '9999'
        }
      end

      before { Record.config = config  }

      it { expect(subject).to eq(config) }
    end

    context 'when providing a configuration with missing keys' do
      before { Record.config = {} }
      it { expect(subject).to eq(config) }
    end
  end
end