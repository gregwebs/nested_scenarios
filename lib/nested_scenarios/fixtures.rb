class Fixtures < (RUBY_VERSION < '1.9' ? YAML::Omap : Hash)
  cattr_accessor :current_fixtures

  def self.destroy_fixtures(table_names)
    NestedScenarios.delete_tables(table_names)
  end
end

module ActiveRecord #:nodoc:
  module TestFixtures #:nodoc:

    def self.included(base)
      base.class_eval do
        setup :setup_fixtures
        teardown :teardown_fixtures

        superclass_delegating_accessor :fixture_path
        superclass_delegating_accessor :fixture_table_names
        superclass_delegating_accessor :fixture_class_names
        superclass_delegating_accessor :use_transactional_fixtures
        superclass_delegating_accessor :use_instantiated_fixtures   # true, false, or :no_instances
        superclass_delegating_accessor :pre_loaded_fixtures

        self.fixture_table_names = []
        self.use_transactional_fixtures = false
        self.use_instantiated_fixtures = true
        self.pre_loaded_fixtures = false

        self.fixture_class_names = {}

        superclass_delegating_accessor :scenario_path
        superclass_delegating_accessor :load_root_fixtures
        superclass_delegating_accessor :root_table_names
        superclass_delegating_accessor :scenario_table_names
        self.load_root_fixtures = false
      end

      base.extend ClassMethods
    end
  
    module ClassMethods
      def scenario(scenario_name = nil, options = {})
        case scenario_name
          when Hash
            self.load_root_fixtures = scenario_name.delete(:root) if scenario_name.key? :root
            scenario_name = scenario_name.join('/')
          when Symbol, String
            self.load_root_fixtures = options.delete(:root) if options.key? :root
            scenario_name = scenario_name.to_s
          else
            raise ArgumentError, "Scenario must be a symbol, string or hash. You gave #{scenario_name.class}."
        end

        self.scenario_path = "#{self.fixture_path}/#{scenario_name}" if scenario_name
        self.fixtures(:all)
      end

      def fixtures(*table_names)
        if table_names.first == :all
          self.root_table_names = self.load_root_fixtures ? load_table_names_in_path(self.fixture_path) : []
          self.scenario_table_names = self.scenario_path ? load_table_names_in_path(self.scenario_path) : []

          table_names = self.root_table_names + self.scenario_table_names
          table_names.uniq!
        else
          table_names = table_names.flatten.map { |n| n.to_s }
        end

        self.fixture_table_names |= table_names

        require_fixture_classes(table_names)
        setup_fixture_accessors(table_names)
      end

      private
        def load_table_names_in_path(path)
          table_names = Dir["#{path}/*.yml"] + Dir["#{path}/*.csv"]
          table_names.map! { |f| File.basename(f).split('.')[0..-2].join('.') }
          return table_names
        end
    end

    private
      def load_fixtures
        @loaded_fixtures = {}

        current_fixtures = self.fixture_path.to_s + self.scenario_path.to_s
        if Fixtures.current_fixtures != current_fixtures
          Fixtures.current_fixtures = current_fixtures
          Fixtures.reset_cache
        end

        if self.scenario_path
          scenario_fixtures = Fixtures.create_fixtures(self.scenario_path, self.scenario_table_names, fixture_class_names)
        end

        if self.load_root_fixtures
          root_fixtures = Fixtures.create_fixtures(self.fixture_path, self.root_table_names, fixture_class_names)
        end

        [root_fixtures, scenario_fixtures].each do |fixtures|
          next if fixtures.nil?

          if fixtures.instance_of?(Fixtures)
            @loaded_fixtures[fixtures.table_name] = fixtures
          else
            fixtures.each { |f| @loaded_fixtures[f.table_name] = f }
          end
        end
      end

      def teardown_fixtures
        return unless defined?(ActiveRecord) && !ActiveRecord::Base.configurations.blank?

        Fixtures.destroy_fixtures(self.fixture_table_names)

        unless run_in_transaction?
          Fixtures.reset_cache
        end

        # Rollback changes if a transaction is active.
        if run_in_transaction? && ActiveRecord::Base.connection.open_transactions != 0
          ActiveRecord::Base.connection.rollback_db_transaction
          ActiveRecord::Base.connection.decrement_open_transactions
        end
        ActiveRecord::Base.clear_active_connections!
      end

  end
end
