module CassandraModel
  class Logging
    #noinspection RubyClassVariableUsageInspection
    @@logger = Logger.new(STDOUT).tap { |logger| logger.level = Logger::WARN }

    cattr_accessor :logger
  end
end