require 'rspec'

module CassandraModel
  describe Logging do

    describe '.logger' do
      subject { Logging.logger }

      it 'should create an instance of logger by default' do
        expect(subject).to be_a_kind_of(Logger)
      end

      it 'should log to STDOUT by default' do
        expect(subject.instance_variable_get(:@logdev).dev).to eq(STDOUT)
      end

      it 'should have a default logging level of WARN' do
        expect(subject.level).to eq(Logger::WARN)
      end

      it 'should cache the logger' do
        expect(subject).to eq(Logging.logger)
      end

      describe 'overriding the default logger' do
        let(:logger) { double(:logger) }

        it 'should allow us to override the default logger' do
          Logging.logger = logger
          is_expected.to eq(logger)
        end
      end
    end

  end
end