# frozen_string_literal: true

require 'http'
require 'json/add/exception'

module Reactor
  module Streams
    class Kafka < Stream
      class Error < StandardError; end
      class ArgumentError < Error; end
      class ProducerError < Error; end

      module RetryFailOptions
        DROP_CONSUMER = :drop_consumer
        ADD_END_OF_QUEUE = :add_end_of_queue
        RUN_BLOCK = :run_block
        RUN_CLASS = :run_class
        IGNORE = :ignore
      end

      attr_reader :subscriptions, :consumer_threads

      # @param [String] topic Topic name of Kafka
      # @param [String] group_id Set a group name for consumers
      # @param [Boolean] run_consumers Will consumer threads begin to work
      # @param [Boolean] start_from_beginning Will consumers start consume from beginning
      # @param [Hash] error_handling_opts
      # @param [Hash] batch_fetching_opts
      # @return [Reactor::Stream] a stream which is subscribable
      #
      # error_handling_opts
      # {
      #   # Number of tries on fail before taking some actions
      #   retry_count_on_fail: 0,
      #
      #   # Maximum number of adding end of queue if it still fails
      #   max_add_end_of_queue_count: 5,
      #
      #   # Action taken when max retry reached
      #   on_max_retry_reached: RetryFailOptions::ADD_END_OF_QUEUE,
      #
      #   # This allows to increase wait time between retries
      #   wait_incrementer: ->(wait) { wait + (wait / 2) },
      #
      #   # If you want to wrap subscriber calls into a wrapper fn like ActiveRecord::Base.transaction
      #   wrapper_proc: nil,
      #
      #   # Initial wait time in seconds
      #   initial_wait: 1,
      #
      #   # If on_max_retry_reached is RetryFailOptions::RUN_BLOCK then this proc will be called with current message
      #   error_proc: nil
      #
      #   # If on_max_retry_reached is RetryFailOptions::RUN_CLASS then this class will be
      #       instantiated with current message and will be called its on_error method.
      #   error_class: nil
      # }
      #
      # batch_fetching_opts
      # {
      #   # Before start fetching messages wait this much bytes to be written to the stream
      #   min_bytes: 1,
      #
      #   # Before sending fetched messages to consumers wait this much bytes to be written to the stream
      #   max_bytes: 10485760,
      #
      #   # Wait this much seconds to next polling. Overrides other two options
      #   max_wait_time: 1
      # }
      def initialize(
        topic,
        group_id: Reactor.configuration.group_id,
        run_consumers: Reactor.configuration.run_consumers,
        start_from_beginning: false,
        config: Reactor.configuration,
        kafka: Reactor.kafka,
        kafka_api: Reactor.kafka_api,
        error_handling_opts: Reactor.configuration.error_handling_opts,
        batch_fetching_opts: {}
      )
        @topic = topic
        @group_id = group_id || config.group_id
        raise ArgumentError.new('Group id should be set') unless @group_id
        @group_id = "#{@group_id}-#{@topic}"

        @error_handling_opts = {
          retry_count_on_fail: 0,
          max_add_end_of_queue_count: 5,
          initial_wait: 1,
          wait_incrementer: ->(wait) { wait + (wait / 2) },
          wrapper_proc: nil,
          error_proc: nil,
          error_class: nil,
          producer_error_proc: nil,
          producer_error_class: nil
        }.merge(error_handling_opts)

        @batch_fetching_opts = {
          min_bytes: 1,
          max_bytes: 10485760,
          max_wait_time: 1
        }.merge(batch_fetching_opts)

        @logger = config.logger
        @start_from_beginning = start_from_beginning
        @kafka = kafka
        @kafka_api = kafka_api
        @consumers = []

        if @error_handling_opts[:retry_count_on_fail].negative?
          raise ArgumentError.new('retry_count_on_fail should be set to 0 or greater')
        end

        @subscriptions = {}

        set_on_max_retry_reached(@error_handling_opts[:on_max_retry_reached])
        @stop_threads = false
        create_consumers(@batch_fetching_opts) if run_consumers || config.run_consumers
      end

      def subscribe_batch(id:, action: Reactor::Reaction::DEFAULT, packet: Reactor::Packet::DEFAULT, &block)
        raise Error.new('You cannot make batch subscription to a Kafka stream due to offset commit limitations.')
      end

      def publish_unique(id, message = nil, force = false, action: Reactor::Reaction::DEFAULT, packet: Reactor::Packet::DEFAULT)
        reactor_params = { action:, packet:, id:, force: }
        producer_message = {
          message: { content: message, __reactor_params__: reactor_params }.to_json,
          id: id.to_s,
          force:
        }
        send_to_producer([producer_message])
      end

      def publish(message = nil, action: Reactor::Reaction::DEFAULT, packet: Reactor::Packet::DEFAULT)
        reactor_params = { action:, packet: }
        producer_message = { message: { content: message, __reactor_params__: reactor_params }.to_json }
        send_to_producer([producer_message])
      end

      def publish_batch(messages = [], action: Reactor::Reaction::DEFAULT, packet: Reactor::Packet::DEFAULT)
        reactor_params = { action:, packet: }
        producer_messages = messages.map do |message|
          { message: { content: message, __reactor_params__: reactor_params }.to_json }
        end
        send_to_producer(producer_messages)
      end

      def unsubscribe(index, action: Reactor::Reaction::DEFAULT, packet: Reactor::Packet::DEFAULT)
        @subscriptions[[action, packet, false]].delete_at(index)
      end

      def unsubscribe_batch(index, action: Reactor::Reaction::DEFAULT, packet: Reactor::Packet::DEFAULT)
        @subscriptions[[action, packet, true]].delete_at(index)
      end

      def stop_consumers
        @logger.info("Stopping consumers for #{@topic}")
        @consumers.each do |consumer|
          consumer.stop
        end
        @stop_threads = true
      end

      private
        def create_consumers(fetching_options)
          consumer_count = @kafka.partitions_for(@topic)
          consumer_options = {
            automatically_mark_as_processed: false,
            min_bytes: fetching_options[:min_bytes],
            max_bytes: fetching_options[:max_bytes],
            max_wait_time: fetching_options[:max_wait_time]
          }
          @consumers = (1..consumer_count).map { @kafka.consumer(group_id: @group_id, session_timeout: 100) }
          @consumer_threads = @consumers.map do |consumer|
            consumer.subscribe(@topic)
            Thread.new do
              consumer.each_batch(**consumer_options) do |batch|
                if batch != nil
                  if @error_handling_opts[:wrapper_proc]
                    @error_handling_opts[:wrapper_proc].call { consume(consumer, batch) }
                  else
                    consume(consumer, batch)
                  end
                end
              end
            end
          end
          trap('TERM') { stop_consumers }
          trap('INT') { stop_consumers }
        end

        def set_on_max_retry_reached(on_max_retry_reached)
          unless [
            RetryFailOptions::DROP_CONSUMER,
            RetryFailOptions::ADD_END_OF_QUEUE,
            RetryFailOptions::IGNORE,
            RetryFailOptions::RUN_BLOCK,
            RetryFailOptions::RUN_CLASS,
          ].include?(on_max_retry_reached)
            msg = 'Invalid on_max_retry_reached parameter please use one of Reactor::Streams::Kafka::RetryFailOptions'
            raise ArgumentError.new(msg)
          end
          if RetryFailOptions::RUN_BLOCK == on_max_retry_reached && @error_handling_opts[:error_proc].nil?
            raise ArgumentError.new('You should set error_proc parameter to run on fail')
          end
          if RetryFailOptions::RUN_CLASS == on_max_retry_reached && @error_handling_opts[:error_class].nil?
            raise ArgumentError.new('You should set error_class parameter to run on fail')
          end
          @on_max_retry_reached = on_max_retry_reached
        end

        def send_to_producer(messages)
          data = { topic: @topic, messages: }
          begin
            try_to_run(3, nil, @error_handling_opts[:initial_wait]) do
              @kafka_api.send_request(data)
            end
          rescue Exception => e
            if @error_handling_opts[:producer_error_class]
              @error_handling_opts[:producer_error_class].new(@topic, messages).on_error(e)
            elsif @error_handling_opts[:producer_error_proc]
              @error_handling_opts[:producer_error_proc].call(@topic, messages, e)
            else
              raise(e)
            end
            @logger.error(e)
          end
        end

        def try_to_run(allowed_count, consumer, wait = 1.0, count = 0, &block)
          begin
            yield
          rescue Exception => e
            consumer.send_heartbeat if consumer
            raise(e) if count == allowed_count
            sleep(wait) if wait > 0
            try_to_run(allowed_count, consumer, @error_handling_opts[:wait_incrementer].call(wait), count + 1, &block)
          end
        end

        def run_failsafe(box, message, proc_id, consumer, &block)
          begin
            try_to_run(@error_handling_opts[:retry_count_on_fail], consumer, @error_handling_opts[:initial_wait], &block)
          rescue Exception => e
            if @on_max_retry_reached == RetryFailOptions::ADD_END_OF_QUEUE
              params = message['__reactor_params__']
              params['retry_count'] ||= 0
              if params['retry_count'] < @error_handling_opts[:max_add_end_of_queue_count]
                params['retry_count'] += 1
                params['group'] ||= @group_id
                params['proc'] ||= proc_id
                params['error'] = e
                producer_message = { message: message.to_json }
                producer_message[:id] = params['id'] if params[:id]
                producer_message[:force] = params['force'] if params[:force]
                send_to_producer([producer_message])
                @logger.info(e)
              else
                redirect_error(box, e)
              end
            elsif @on_max_retry_reached == RetryFailOptions::DROP_CONSUMER
              @logger.error(e)
              raise(e)
            elsif @on_max_retry_reached == RetryFailOptions::IGNORE
              @logger.info(e)
            elsif @on_max_retry_reached == RetryFailOptions::RUN_BLOCK
              @error_handling_opts[:error_proc].call(@topic, box, e)
              @logger.info(e)
            elsif @on_max_retry_reached == RetryFailOptions::RUN_CLASS
              @error_handling_opts[:error_class].new(@topic, box).on_error(e)
              @logger.info(e)
            end
          end
        end

        def commit_offset(consumer, box)
          consumer.mark_message_as_processed(box)
          consumer.commit_offsets
        end

        def redirect_error(box, error)
          if @error_handling_opts[:error_class]
            @error_handling_opts[:error_class].new(@topic, box).on_error(error)
          elsif @error_handling_opts[:error_proc]
            @error_handling_opts[:error_proc].call(@topic, box, error)
          else
            @logger.error(error)
            raise(error)
          end
        end

        def consume(consumer, batch)
          batch.messages.each do |box|
            message = JSON.parse(box.value)
            params = message['__reactor_params__']
            action, packet = params.values_at('action', 'packet').map(&:to_sym)
            group, proc_id = params.values_at('group', 'proc')
            if group.nil? || group == @group_id
              subscription_types(action, packet, false)
                .select { |key| subscriptions.has_key?(key) }
                .each do |key|
                  at_least_one_proc_called = false
                  subscriptions[key].each do |act|
                    next unless proc_id.nil? || act[:id] == proc_id
                    at_least_one_proc_called = true
                    run_failsafe(box, message, act[:id], consumer) do
                      act[:proc].call(message['content'])
                    end
                  end
                  if proc_id && !at_least_one_proc_called
                    error = params['error']['json_class'].constantize.json_create(params['error'])
                    redirect_error(box, error)
                    @logger.info(error)
                  end
                end
            end
            try_to_run(2, consumer, 1) { commit_offset(consumer, box) }
          end
        end
    end
  end
end
