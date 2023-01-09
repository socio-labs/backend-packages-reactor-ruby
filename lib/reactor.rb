# frozen_string_literal: true

require 'ruby-kafka'
require 'active_support'
require_relative 'configuration'
require_relative 'reactor/packet'
require_relative 'reactor/consumer'
require_relative 'reactor/reaction'
require_relative 'reactor/stream'
require_relative 'reactor/version'
require_relative 'hacks'
require_relative 'reactor/kafka/api'
require_relative 'reactor/streams/kafka'

module Reactor
  class Error < StandardError; end

  class << self
    attr_reader :kafka, :kafka_api

    def configuration
      @configuration ||= Configuration.new
    end

    def configure
      yield(configuration)

      @kafka = ::Kafka::Client.new(**configuration.ruby_kafka.merge(logger: configuration.logger))

      kafka_api_conf = configuration.kafka_api
      api_message_url = kafka_api_conf[:url] + kafka_api_conf[:create_message_uri]
      @kafka_api = Kafka::Api.new(api_message_url, kafka_api_conf[:project], kafka_api_conf[:code], configuration.logger)
    end
  end
end
