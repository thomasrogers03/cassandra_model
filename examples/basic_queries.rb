require_relative 'common'

class ImageData < Record
  ImageData.primary_key = [:path, :data]
  ImageData.columns = [:path, :data]
end

Record.config = { hosts: %w(me), keyspace: 'pcrawler' }
puts ImageData.where(limit: 10).map(&:path)
