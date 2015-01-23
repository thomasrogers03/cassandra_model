require_relative 'common'

class ImageData < Record
  ImageData.primary_key = [:path, :data]
  ImageData.columns = [:path, :data]
end

Record.config = { hosts: %w(me), keyspace: 'pcrawler' }
ImageData.where(limit: 1000, page_size: 10).each { |row| puts row.path }
