# frozen_string_literal: true

module Karafka
  # Karafka consuming server class
  class Server
    class << self
      # Set of consuming threads. Each consumer thread contains a single consumer
      attr_accessor :consumer_threads

      # Writer for list of consumer groups that we want to consume in our current process context
      attr_writer :consumer_groups

      # Method which runs app
      def run
        @consumer_threads = Concurrent::Array.new

        %i[sigint sigquit sigterm].each do |s|
          process.send("on_#{s}", &method(:bind_exit_signals))
        end

        process.on_sighup(&method(:bind_restart_signals))

        start_supervised
      end

      # @return [Array<String>] array with names of consumer groups that should be consumed in a
      #   current server context
      def consumer_groups
        # If not specified, a server will listed on all the topics
        @consumer_groups ||= Karafka::App.consumer_groups.map(&:name).freeze
      end

      private

      # When receiving a signal to exit, what do we do?
      def bind_exit_signals
        Karafka::App.stop!
      end

      # When receiving a signal to restart, what do we do?
      def bind_restart_signals
        Karafka::App.stop!
      end

      # @return [Karafka::Process] process wrapper instance used to catch system signal calls
      def process
        Karafka::Process.instance
      end

      # Starts Karafka with a supervision
      # @note We don't need to sleep because Karafka::Fetcher is locking and waiting to
      # finish loop (and it won't happen until we explicitily want to stop)
      def start_supervised
        process.supervise do
          Karafka::App.run!
          Karafka::Fetcher.new.fetch_loop
        end
      end
    end
  end
end
