# frozen_string_literal: true

require 'logger'

module Reactor
  class Configuration
    attr_accessor :logger, :run_consumers, :error_handling_opts, :group_id, :kafka_api, :ruby_kafka

    def initialize
      @logger = ::Logger.new(STDOUT)
      @run_consumers = ENV['CONSUMER_CLUSTER'].present?
      @group_id = nil
      @error_handling_opts = {
        retry_count_on_fail: 0,
        on_max_retry_reached: Streams::Kafka::RetryFailOptions::DROP_CONSUMER
      }

      @kafka_api = {}

      @ruby_kafka = {
        # the list of brokers used to initialize the client. Either an Array of connections,
        # or a comma separated string of connections. A connection can either be a string of
        # "host:port" or a full URI with a scheme. If there's a scheme it's ignored and only
        # host/port are used.
        seed_brokers: %w[kafka://127.0.0.1:9092],

        # the identifier for this application.
        client_id: 'my_application',

        # the timeout setting for connecting to brokers.
        connect_timeout: nil,

        # the timeout setting for socket connections.
        socket_timeout: nil,

        # whether to resolve each hostname of the seed brokers. If a broker is resolved to
        # multiple IP addresses, the client tries to connect to each of the addresses until
        # it can connect.
        resolve_seed_brokers: false
      }
    end
  end
end
