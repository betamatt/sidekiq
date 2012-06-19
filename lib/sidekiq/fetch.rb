require 'sidekiq'
require 'celluloid'

module Sidekiq
  ##
  # The Fetcher blocks on Redis, waiting for a message to process
  # from the queues.  It gets the message and hands it to the Manager
  # to assign to a ready Processor.
  class Fetcher
    include Celluloid
    include Sidekiq::Util

    TIMEOUT = 1

    def initialize(mgr, queues)
      @mgr = mgr
      @queues = queues.map { |q| "queue:#{q}" }
      @unique_queues = @queues.uniq

      # TODO: support multiple queues
      @queue = Sidekiq.sqs { |sqs| sqs.queues.named(queues.first) }
      puts @queue.inspect
    end

    # Fetching is straightforward: the Manager makes a fetch
    # request for each idle processor when Sidekiq starts and
    # then issues a new fetch request every time a Processor
    # finishes a message.
    #
    # Because we have to shut down cleanly, we can't block
    # forever and we can't loop forever.  Instead we reschedule
    # a new fetch if the current fetch turned up nothing.
    def fetch
      watchdog('Fetcher#fetch died') do
        return if Sidekiq::Fetcher.done?

        begin  
          @queue.poll(:batch_size => 1, :poll_interval => 0) do |msg|
            puts "fetched #{msg.inspect}"
            @mgr.assign!(msg)
          end
          
          # Use Celluloid's sleep instead of letting #poll do a Kernel sleep
          sleep 5
        rescue => ex
          logger.error("Error fetching message: #{ex}")
          logger.error(ex.backtrace.first)
          sleep(TIMEOUT)
          after(0) { fetch }
        end
      end
    end

    # Ugh.  Say hello to a bloody hack.
    # Can't find a clean way to get the fetcher to just stop processing
    # its mailbox when shutdown starts.
    def self.done!
      @done = true
    end

    def self.done?
      @done
    end

  end
end
