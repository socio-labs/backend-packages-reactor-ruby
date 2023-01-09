# frozen_string_literal: true

require 'json'
require 'json/add/exception'

RSpec.describe Reactor::Streams::Kafka do
  let!(:stub_create_message) do
    stub_request(:post, 'http://127.0.0.1:8080/api/batch-message').
      with(
        body: /content/,
        headers: { 'code' => 'some-secure-code', 'project' => 'project' }
      ).to_return({ status: 200, body: { code: 0, message: 'testing' }.to_json })
  end

  let(:stubbed_config) do
    config = double
    allow(config).to receive(:group_id) { 'group' }
    allow(config).to receive(:run_consumers) { true }
    allow(config).to receive(:logger) { Logger.new(STDOUT, level: :info) }
    allow(config).to receive(:kafka_api) do
      {
        url: 'http://127.0.0.1:8080/api',
        create_message_uri: '/batch-message',
        project: 'project',
        code: 'some-secure-code'
      }
    end
    allow(config).to receive(:ruby_kafka) do
      {
        client_id: 'project',
        seed_brokers: %w[kafka://127.0.0.1:9092]
      }
    end
    config
  end

  let(:kafka_api) do
    kafka_api_conf = stubbed_config.kafka_api
    api_message_url = kafka_api_conf[:url] + kafka_api_conf[:create_message_uri]
    Reactor::Kafka::Api.new(api_message_url, kafka_api_conf[:project], kafka_api_conf[:code], stubbed_config.logger)
  end

  def kafka_stubber
    kafka = double
    allow(kafka).to receive(:partitions_for) { 1 }
    allow(kafka).to receive(:consumer) {
      consumer = double
      allow(consumer).to receive(:mark_message_as_processed)
      allow(consumer).to receive(:subscribe)
      allow(consumer).to receive(:commit_offsets)
      allow(consumer).to receive(:send_heartbeat)
      batch = double
      allow(batch).to receive(:messages) {
        yield
      }
      allow(consumer).to receive(:each_batch).and_yield(batch)
      consumer
    }
    kafka
  end

  let(:with_normal_messages) do
    kafka_stubber {
      message = double
      allow(message).to receive(:value) {
        { content: 'This is a kafka message',
          __reactor_params__: { action: :default, packet: :default } }.to_json
      }
      [message, message]
    }
  end

  let(:with_later_added_messages) do
    kafka_stubber {
      message = double
      allow(message).to receive(:value) {
        { content: 'This was a kafka message',
          __reactor_params__: {
            action: :default,
            packet: :default,
            retry_count: 1,
            group: 'group-myTopic',
            proc: 'just-a-test-id',
            error: { 'json_class' => 'Reactor::Streams::Kafka::Error', 'm' => 'a previous error was here', 'b' => nil }
          } }.to_json
      }
      [message]
    }
  end

  describe 'argument errors' do
    let(:error_type) { Reactor::Streams::Kafka::ArgumentError }
    subject { -> { Reactor::Streams::Kafka.new('myTopic', **params) } }

    context 'on negative retry_count_on_fail option provided' do
      let(:params) { { error_handling_opts: { retry_count_on_fail: -4 }, config: stubbed_config, kafka: with_normal_messages } }
      it { is_expected.to raise_error(error_type, 'retry_count_on_fail should be set to 0 or greater') }
    end

    context 'on wrong retry fail option provided' do
      let(:params) { { error_handling_opts: { on_max_retry_reached: :unknown }, config: stubbed_config, kafka: with_normal_messages } }
      let(:msg) { 'Invalid on_max_retry_reached parameter please use one of Reactor::Streams::Kafka::RetryFailOptions' }
      it { is_expected.to raise_error(error_type, msg) }
    end

    context 'on on_max_retry_reached set as RUN_BLOCK but block is not provided ' do
      let(:params) do
        {
          error_handling_opts: {
            on_max_retry_reached: Reactor::Streams::Kafka::RetryFailOptions::RUN_BLOCK
          },
          config: stubbed_config,
          kafka: with_normal_messages
        }
      end
      let(:msg) { 'You should set error_proc parameter to run on fail' }
      it { is_expected.to raise_error(error_type, msg) }
    end

    context 'on on_max_retry_reached set as RUN_CLASS but class is not provided ' do
      let(:params) do
        {
          error_handling_opts: {
            on_max_retry_reached: Reactor::Streams::Kafka::RetryFailOptions::RUN_CLASS
          },
          config: stubbed_config,
          kafka: with_normal_messages
        }
      end
      let(:msg) { 'You should set error_class parameter to run on fail' }
      it { is_expected.to raise_error(error_type, msg) }
    end
  end

  describe 'producer' do
    context 'publish' do
      let(:params) { { config: stubbed_config, kafka: with_normal_messages, kafka_api:, run_consumers: false } }
      let(:kafka_stream) { Reactor::Streams::Kafka.new('myTopic', **params) }

      it 'should publish' do
        kafka_stream.publish('test')
        assert_requested(:post, 'http://127.0.0.1:8080/api/batch-message', times: 1)
      end

      it 'should publish batch' do
        expect(kafka_stream).to receive(:send_to_producer).with([
          { message: '{"content":"test1","__reactor_params__":{"action":"action","packet":"packet"}}' },
          { message: '{"content":"test2","__reactor_params__":{"action":"action","packet":"packet"}}' }
        ])
        kafka_stream.publish_batch(%w[test1 test2], action: :action, packet: :packet)
      end

      it 'should publish unique' do
        expect(kafka_stream).to receive(:send_to_producer).with([{
          force: false,
          id: '34',
          message: '{"content":"test1","__reactor_params__":{"action":"action","packet":"packet","id":34,"force":false}}'
        }])
        kafka_stream.publish_unique(34, 'test1', action: :action, packet: :packet)
      end
    end
  end

  describe 'scenarios' do
    context 'with default parameters' do
      before do
        subscriptions = { [:default, :default, false] => [{ id: 'just-a-test-id', proc: ->(msg) { print msg } }] }
        allow_any_instance_of(Reactor::Streams::Kafka).to receive(:subscriptions).and_return(subscriptions)
      end

      let(:kafka_stream) { Reactor::Streams::Kafka.new('myTopic', config: stubbed_config, kafka: with_normal_messages) }

      it 'should handle a single consumer' do
        output = 'This is a kafka messageThis is a kafka message'
        expect { kafka_stream.consumer_threads.each { _1.join(0.1) } }.to output(output).to_stdout
      end
    end

    context 'on error with drop consumer option' do
      before do
        subscriptions = { [:default, :default, false] => [{ id: 'just-a-test-id', proc: ->(_msg) { raise StandardError.new('noo') } }] }
        allow_any_instance_of(Reactor::Streams::Kafka).to receive(:subscriptions).and_return(subscriptions)
      end

      let(:kafka_stream) do
        params = {
          error_handling_opts: { on_max_retry_reached: Reactor::Streams::Kafka::RetryFailOptions::DROP_CONSUMER },
          config: stubbed_config,
          kafka: with_normal_messages
        }
        Reactor::Streams::Kafka.new('myTopic', **params)
      end

      it 'should raise exception' do
        expect do
          kafka_stream.consumer_threads.each { _1.join(0.1) }
        end.to raise_error(StandardError, 'noo')
      end
    end

    context 'when there is a previous message added to queue success this time' do
      before do
        subscriptions = { [:default, :default, false] => [{ id: 'just-a-test-id', proc: ->(msg) { print msg } }] }
        allow_any_instance_of(Reactor::Streams::Kafka).to receive(:subscriptions).and_return(subscriptions)
      end

      let(:kafka_stream) do
        params = { config: stubbed_config, kafka: with_later_added_messages }
        Reactor::Streams::Kafka.new('myTopic', **params)
      end

      it 'should run fine' do
        output = 'This was a kafka message'
        expect { kafka_stream.consumer_threads.each { _1.join(0.1) } }.to output(output).to_stdout
      end
    end

    context 'when there is a previous message added to queue fails again' do
      before do
        subscriptions = { [:default, :default, false] => [{ id: 'just-a-test-id', proc: ->(_msg) { raise StandardError.new('omg') } }] }
        allow_any_instance_of(Reactor::Streams::Kafka).to receive(:subscriptions).and_return(subscriptions)
      end

      let(:kafka_stream) do
        params = {
          error_handling_opts: {
            max_add_end_of_queue_count: 2,
            on_max_retry_reached: Reactor::Streams::Kafka::RetryFailOptions::ADD_END_OF_QUEUE
          },
          config: stubbed_config,
          kafka: with_later_added_messages,
          kafka_api:
        }
        Reactor::Streams::Kafka.new('myTopic', **params)
      end

      it 'should add to queue again' do
        kafka_stream.consumer_threads.each { _1.join(0.1) }
        assert_requested(:post, 'http://127.0.0.1:8080/api/batch-message', times: 1) do |req|
          %w["retry_count\":2 omg].all? { |msg| req.body.include?(msg) }
        end
      end
    end

    context 'when there is a previous message added to queue fails again but add limit reached' do
      before do
        subscriptions = { [:default, :default, false] => [{ id: 'just-a-test-id', proc: ->(_msg) { raise StandardError.new('omg') } }] }
        allow_any_instance_of(Reactor::Streams::Kafka).to receive(:subscriptions).and_return(subscriptions)
      end

      let(:params) do
        {
          error_handling_opts: {
            max_add_end_of_queue_count: 1,
            on_max_retry_reached: Reactor::Streams::Kafka::RetryFailOptions::ADD_END_OF_QUEUE
          },
          config: stubbed_config,
          kafka: with_later_added_messages
        }
      end

      context 'when there is no error proc or error class' do
        let(:kafka_stream) do
          Reactor::Streams::Kafka.new('myTopic', **params)
        end

        it 'should raise error' do
          expect { kafka_stream.consumer_threads.each { _1.join(0.1) } }
            .to raise_error(
              StandardError, 'omg'
            )
        end
      end

      context 'when there is error proc' do
        let(:params_w_proc) do
          params.tap { |p| p[:error_handling_opts][:error_proc] = ->(_topic, box, err) do
            print [JSON.parse(box.value)['content'], err.message]
          end }
        end

        let(:kafka_stream) do
          Reactor::Streams::Kafka.new('myTopic', **params_w_proc)
        end

        it 'should redirect error to error proc' do
          out = '["This was a kafka message", "omg"]'
          expect { kafka_stream.consumer_threads.each { _1.join(0.1) } }.to output(out).to_stdout
        end
      end

      context 'when there is error class' do
        let(:error_class) do
          Class.new do
            def initialize(_topic, box)
              @box = box
            end

            def on_error(error)
              print [JSON.parse(@box.value)['content'], error.message]
            end
          end
        end
        let(:params_w_proc) do
          params.tap { |p| p[:error_handling_opts][:error_class] = error_class }
        end

        let(:kafka_stream) do
          Reactor::Streams::Kafka.new('myTopic', **params_w_proc)
        end

        it 'should redirect error to error proc' do
          out = '["This was a kafka message", "omg"]'
          expect { kafka_stream.consumer_threads.each { _1.join(0.1) } }.to output(out).to_stdout
        end
      end
    end

    context 'wait incrementer' do
      before do
        subscriptions = { [:default, :default, false] => [{ id: 'just-a-test-id', proc: ->(_msg) { raise StandardError.new('noo') } }] }
        allow_any_instance_of(Reactor::Streams::Kafka).to receive(:subscriptions).and_return(subscriptions)
      end

      let(:kafka_stream) do
        params = {
          error_handling_opts: {
            retry_count_on_fail: 3,
            on_max_retry_reached: Reactor::Streams::Kafka::RetryFailOptions::DROP_CONSUMER,
          },
          config: stubbed_config,
          kafka: with_normal_messages,
          kafka_api:
        }
        Reactor::Streams::Kafka.new('myTopic', **params)
      end

      it 'should raise exception' do
        first_time = Time.now.to_i
        expect do
          kafka_stream.consumer_threads[0].join
        end.to raise_error(StandardError, 'noo')
        finish_time = Time.now.to_i
        expect(finish_time - first_time > 2).to be_truthy
      end
    end

    context 'batch subscription' do
      let(:kafka_stream) do
        Reactor::Streams::Kafka.new('myTopic', config: stubbed_config, kafka: with_normal_messages)
      end

      it 'should fail in batch subscription' do
        expect { kafka_stream.subscribe_batch(id: 'some id') {} }.to raise_error(
          Reactor::Streams::Kafka::Error, 'You cannot make batch subscription to a Kafka stream due to offset commit limitations.'
        )
      end
    end
  end
end
