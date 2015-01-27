require_relative 'common'

class ImageData < CassandraModel::Record
  ImageData.primary_key = [:path, :data]
  ImageData.columns = [:path, :data]
end

CassandraModel::Record.config = { hosts: %w(me), keyspace: 'pcrawler' }
ImageData.where(limit: 100, page_size: 10).each.with_index { |row, index| puts "#{index} -> #{row.path}" }
