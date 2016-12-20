require 'csv'

class ParallelCSV
  DEFAULTS = {
    chunk_size: 5_000,
    col_sep: '¦',
    process_num: 4 # Число процессов
  }.freeze

  attr_reader :path, :options

  def initialize(path, options = {})
    @options = DEFAULTS.merge(options)
    @path = path
  end

  def process(&block)
    # Если файл не читается, нужно кинуть эксепшн из родительского процесса
    File.open(path) { }

    offset = 0

    # Пайп для статуса от дочерних процессов
    reader, writer = IO.pipe
    eof = false

    until eof do
      pids = []
      options[:process_num].times do
        pids << fork_unless_test do
          single_process(offset, writer, &block)
        end

        offset += options[:chunk_size]
      end

      pids.compact.each { |pid| Process.wait(pid) }

      # Читаем из пайпа статус, избегая блокировки
      # Если хотя бы один процесс кинул stop, останавливаемся
      eof = (IO.select([reader], [writer])[0].length > 0 && reader.gets == "stop\n")
    end
  end

  private

  def single_process(offset, writer, &block)
    io = IO.new(IO.sysopen(path, 'r'), 'r')
    csv = CSV.new(io, csv_options)
    chunk = []

    # Проматываем до offset строки
    offset.times { io.readline }

    # Читаем chunk_size строк или пока не встретим EOF
    options[:chunk_size].times do
      line = csv.readline
      chunk << line.fields if line

      # Exception is not for control flow
      if io.eof?
        writer.puts 'stop'
        break
      end
    end
  rescue StandardError => e
    # STDOUT.puts e.message
    writer.puts 'stop'
  ensure
    # Что бы ни случилось, пытаемся отдать чанк
    yield chunk if block_given? && !chunk.empty?
  end

  def csv_options
    options.except(:process_num, :chunk_size)
  end

  # Залепуха для RSpec
  def fork_unless_test
    if Rails.env == 'test'
      yield
      return nil
    else
      fork { yield }
    end
  end
end
