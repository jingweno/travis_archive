require "time"
require "influxdb"
require "pusher-client"
require "logger"

module TravisArchive
  class Crawler
    def self.start!
      logger = Logger.new(STDOUT)
      logger.level = Logger::DEBUG

      db_config = {
        :host => ENV["DB_HOST"],
        :port => ENV["DB_PORT"].to_i,
        :username => ENV["DB_USERNAME"],
        :password => ENV["DB_PASSWORD"]
      }
      self.new(ENV["PUSHER_TOKEN"], db_config, logger).start!
    end

    def initialize(pusher_token, db_config, logger)
      PusherClient.logger = logger

      @pusher_token = pusher_token
      @db = InfluxDB::Client.new "travis", db_config.merge(:async => true, :retry => true)
      @logger = logger
    end

    def start!
      socket = PusherClient::Socket.new(@pusher_token)
      socket.subscribe("common")

      events = ["build:created", "build:started", "build:finished", "build:canceled"]
      events.each do |event|
        socket.bind(event) do |data|
          handle(event, data)
        end
      end

      socket.connect
    end

    private

    def handle(event, data)
      begin
        hash = JSON.parse(data)
        name, point = build_point(event, hash)

        @logger.info("[#{event}]: #{point}")

        @db.write_point(name, point)
        @db.write_point("all", point)
      rescue => e
        @logger.error("Crawler error: #{e.inspect}")
      end
    end

    def build_point(event, hash)
      build = decode_field(hash, "build")
      commit = decode_field(hash, "commit")
      repo = decode_field(hash, "repository")

      now = Time.now.to_i
      point = build.merge(commit).merge(repo)
      name = point["repository_slug"].gsub("/", ".")
      point = point.merge(
        "name" => name,
        "time" => point["build_started_at"] || now,
        "sequence_number" => point["build_finished_at"] || point["build_started_at"] || now,
        "event" => event
      )

      [ name, point ]
    end

    def decode_field(hash, name)
      rows = hash[name].map do |k, v|
        v = Time.parse(v).to_i if k =~ /_at$/ && v
        [ "#{name}_#{k}", v ]
      end

      Hash[rows]
    end
  end
end
