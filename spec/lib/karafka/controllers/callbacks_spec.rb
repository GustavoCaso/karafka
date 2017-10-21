# frozen_string_literal: true

RSpec.describe Karafka::Controllers::Callbacks do
  subject(:base_controller) { working_class.new }

  let(:topic_name) { "topic#{rand}" }
  let(:backend) { :inline }
  let(:responder_class) { nil }
  let(:consumer_group) { Karafka::Routing::ConsumerGroup.new(rand.to_s) }
  let(:topic) do
    topic = Karafka::Routing::Topic.new(topic_name, consumer_group)
    topic.controller = Class.new(Karafka::BaseController)
    topic.controller.include Karafka::Controllers::Callbacks
    topic.backend = backend
    topic.responder = responder_class
    topic
  end
  let(:working_class) do
    ClassBuilder.inherit(Karafka::BaseController) do
      include Karafka::Backends::Inline
      include Karafka::Controllers::Responders
      include Karafka::Controllers::Callbacks

      def perform
        self
      end
    end
  end

  before { working_class.topic = topic }

  describe '#perform' do
    let(:working_class) { ClassBuilder.inherit(Karafka::BaseController) }

    it { expect { base_controller.send(:perform) }.to raise_error NotImplementedError }
  end

  context 'after_received' do
    describe '#call' do
      context 'when there are no callbacks' do
        it 'just schedules' do
          expect(base_controller).to receive(:process)

          base_controller.call
        end
      end
    end

    context 'when we have a block based after_received' do
      let(:backend) { :inline }

      context 'and it throws abort to halt' do
        subject(:base_controller) do
          ClassBuilder.inherit(Karafka::BaseController) do
            include Karafka::Backends::Inline
            include Karafka::Controllers::Callbacks

            after_received do
              throw(:abort)
            end

            def perform
              self
            end
          end.new
        end

        it 'does not perform' do
          expect(base_controller).not_to receive(:perform)

          base_controller.call
        end
      end

      context 'and it does not throw abort to halt' do
        subject(:base_controller) do
          ClassBuilder.inherit(Karafka::BaseController) do
            include Karafka::Backends::Inline
            include Karafka::Controllers::Callbacks

            after_received do
              true
            end

            def perform
              self
            end
          end.new
        end

        let(:params) { double }

        it 'executes' do
          expect(base_controller).to receive(:process)
          base_controller.call
        end
      end
    end

    context 'when we have a method based after_received' do
      let(:backend) { :inline }

      context 'and it throws abort to halt' do
        subject(:base_controller) do
          ClassBuilder.inherit(Karafka::BaseController) do
            include Karafka::Controllers::Callbacks

            after_received :method

            def perform
              self
            end

            def method
              throw(:abort)
            end
          end.new
        end

        it 'does not perform' do
          expect(base_controller).not_to receive(:perform)

          base_controller.call
        end
      end

      context 'and it does not return false' do
        subject(:base_controller) do
          ClassBuilder.inherit(Karafka::BaseController) do
            include Karafka::Backends::Inline
            include Karafka::Controllers::Callbacks

            after_received :method

            def perform
              self
            end

            def method
              true
            end
          end.new
        end

        it 'schedules to a backend' do
          expect(base_controller).to receive(:process)

          base_controller.call
        end
      end
    end
  end
end