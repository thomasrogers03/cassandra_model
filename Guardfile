guard :rspec, cmd: 'bundle exec rspec' do
  watch(%r{^spec/.+_spec\.rb$})
  watch(%r{^lib/(.+)\.rb$}) { |m| "spec/lib/#{m[1]}_spec.rb" }
  watch(%r{^lib/cassandra_model\.rb$}) { 'spec' }
  watch(%r{^lib/cassandra_model/(.+)\.rb}) { |m| "spec/classes/#{m[1]}_spec.rb" }
  watch(%r{^lib/cassandra_model/table_debug\.rb}) { |_| 'spec/classes/table_redux_spec.rb' }
  watch(%r{^lib/cassandra_model/table_debug\.rb}) { |_| 'spec/classes/meta_spec.rb' }
  watch(%r{^lib/cassandra_model/table_debug\.rb}) { |_| 'spec/classes/rotating_table_spec.rb' }
  watch('spec/spec_helper.rb') { 'spec' }
  watch(%r{^spec/shared_examples/(.+)\.rb}) { 'spec' }
  watch(%r{^spec/helpers/(.+)\.rb}) { 'spec' }
  watch(%r{^spec/support/(.+)\.rb}) { 'spec' }
end

guard :rspec, cmd: 'bundle exec rspec', spec_paths: %w(spec/v2) do
  watch(%r{^spec/v2/.+_spec\.rb$})
  watch(%r{^lib/cassandra_model\.rb$}) { 'spec/v2' }
  watch(%r{^lib/cassandra_model/v2/(.+)\.rb}) { |m| "spec/v2/classes/#{m[1]}_spec.rb" }
  watch('spec/v2_spec_helper.rb') { 'spec/v2' }
  watch(%r{^spec/v2/shared_examples/(.+)\.rb}) { 'spec/v2' }
  watch(%r{^spec/v2/helpers/(.+)\.rb}) { 'spec/v2' }
  watch(%r{^spec/v2/support/(.+)\.rb}) { 'spec/v2' }
end

guard :rspec, cmd: 'bundle exec rspec', spec_paths: %w(integration) do
  watch(%r{^integration/.+_spec\.rb$})
  watch('spec/integration_spec_helper.rb') { 'integration' }
end

guard :bundler do
  require 'guard/bundler'
  require 'guard/bundler/verify'
  helper = Guard::Bundler::Verify.new

  files = ['Gemfile']
  files += Dir['*.gemspec'] if files.any? { |f| helper.uses_gemspec?(f) }

  files.each { |file| watch(helper.real_path(file)) }
end
