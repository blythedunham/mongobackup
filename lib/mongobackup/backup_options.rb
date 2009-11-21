module EY::Flex
  class MongoBackups
    def self.run(args)
      options = {:config => '/etc/.mongodb.backups.yml',
                 :command => :new_backup}

      # Build a parser for the command line arguments
      opts = OptionParser.new do |opts|
        opts.version = "0.0.1"

        opts.banner = "Usage: mongobackup [-flag] [argument]"
        opts.define_head "mongobackup: backing up your shit since way back when..."
        opts.separator '*'*80
        
        opts.on("-l", "--list-backup DATABASE", "List mysql backups for DATABASE") do |db|
          options[:db] = (db || 'all')
          options[:command] = :list
        end
        
        opts.on("-n", "--new-backup", "Create new mysql backup") do
          options[:command] = :new_backup
        end
        
        opts.on("-c", "--config CONFIG", "Use config file.") do |config|
          options[:config] = config
        end
        
        opts.on("-d", "--download BACKUP_INDEX", "download the backup specified by index. Run eybackup -l to get the index.") do |index|
          options[:command] = :download
          options[:index] = index
        end

        opts.on("-e", "--engine DATABASE_ENGINE", "The database engine. ex: mysql, postgres, mongodb.") do |engine|
          options[:engine] = engine || 'mongo'
          options[:config] ||= '/etc/.#{options[:engine]}.backups.yml'
        end

        opts.on("-r", "--restore BACKUP_INDEX", "Download and apply the backup specified by index WARNING! will overwrite the current db with the backup. Run eybackup -l to get the index.") do |index|
          options[:command] = :restore
          options[:index] = index
        end

      end

      opts.parse!(args)

      eyb = nil
      if File.exist?(options[:config])
        eyb = case options[:engine]
          when 'postgres'         then EyBackup::PostgresqlBackup.new(YAML::load(File.read(options[:config])))
          when 'mysql'  then EyBackup::MysqlBackup.new(YAML::load(File.read(options[:config])))
          when 'mongo', 'mongodb', NilClass then EyBackup::MongoBackup.new(YAML::load(File.read(options[:config])))
          else raise "Invalid engine: #{options[:engine]}"
          end
      else
        puts "You need to have a mongo backup file at #{options[:config]}"
        exit 1
      end

      case options[:command]
      when :list
        eyb.list options[:db], true
      when :new_backup
        eyb.new_backup
      when :download
        eyb.download(options[:index])
      when :restore
        eyb.restore(options[:index])
      end
      eyb.cleanup
    end
  end
end