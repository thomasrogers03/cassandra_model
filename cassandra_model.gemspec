Gem::Specification.new do |s|
  s.name = 'cassandra_model'
  s.version = '0.9.3'
  s.license = 'Apache License 2.0'
  s.summary = ''
  s.description = ''
  s.authors = ['Thomas RM Rogers']
  s.email = 'thomasrogers03@gmail.com'
  s.files = Dir['{lib}/**/*.rb', 'bin/*', 'LICENSE.txt', '*.md']
  s.require_path = 'lib'
  s.homepage = ''
  s.add_runtime_dependency 'cassandra-driver', '~> 1.1'
  s.add_runtime_dependency 'activesupport', '>= 4.0'
  s.add_runtime_dependency 'thomas_utils', '>= 0.1.13'
end
