Gem::Specification.new do |s|
  s.name = 'cassandra_model'
  s.version = '1.1.8'
  s.license = 'Apache License 2.0'
  s.summary = 'Cassandra data modelling framework for Ruby'
  s.description = %q{Cassandra data modelling framework for Ruby that makes
data modelling for Cassandra tables easy, fast, and stable}
  s.authors = ['Thomas RM Rogers']
  s.email = 'thomasrogers03@gmail.com'
  s.files = Dir['{lib}/**/*.rb', 'bin/*', 'LICENSE.txt', '*.md']
  s.require_path = 'lib'
  s.homepage = 'https://www.github.com/thomasrogers03/cassandra_model'
  s.add_runtime_dependency 'cassandra-driver', '~> 2.0.0'
  s.add_runtime_dependency 'activesupport', '~> 4.0'
  s.add_runtime_dependency 'batch_reactor', '~> 0.1.0'
  s.add_runtime_dependency 'thomas_utils', '>= 0.2.15', '<= 0.3'
  s.add_runtime_dependency 'concurrent-ruby', '~> 1.0.0'
end
