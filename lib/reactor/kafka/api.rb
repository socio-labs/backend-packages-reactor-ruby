# frozen_string_literal: true

require 'http'
require 'json/add/exception'

module Reactor
  module Kafka
    class Api

      class Error < StandardError; end

      def initialize(create_message_url, project, code, logger)
        @create_message_url = create_message_url
        @logger = logger
        @headers = {
          'Content-Type' => 'application/json',
          'Accept' => 'application/json',
          'project' => project,
          'code' => code
        }
      end

      def send_to_producer(topic, messages)
        begin
          send_request({ topic: topic, messages: messages })
          true
        rescue Exception => e
          @logger.error(e)
          false
        end
      end

      def send_request(data)
        request = HTTP.timeout(connect: 10, read: 10).headers(@headers)
        # @logger.info('Request') { request }
        response = request.post(@create_message_url, {json: data})
        body = response.to_s
        # @logger.info('Response') { response }
        check_http_error(response.status, body)
        check_client_error(body)
      end

      private

        def check_http_error(status, body)
          unless status.success?
            message = status.to_s + ' : ' + body
            raise Error.new(message)
          end
        end

        def check_client_error(body)
          body_as_hash = JSON.parse(body) rescue raise(Error.new('Response body is not a valid JSON'))
          raise Error.new('Response body has no code field') unless body_as_hash.key?('code')
          raise Error.new('Response body has no message field') unless body_as_hash.key?('message')
          if body_as_hash['code'] != 0
            message = body_as_hash['code'].to_s + ' : ' + body_as_hash['message']
            raise Error.new(message)
          end
        end
    end
  end
end

