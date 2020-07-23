module ActiveRecord
  module Associations
    class Preloader
      class Association
        def query_scope(ids)
          # CPK
          # scope.where(association_key.in(ids))

          if reflection.foreign_key.is_a?(Array)
            predicate = cpk_in_predicate(table, reflection.foreign_key, ids)
            scope.where(predicate)
          else
            scope.where(association_key.in(ids))
          end
        end

        def associated_records_by_owner(preloader)
          owners_map = owners_by_key
          # CPK
          # owner_keys = owners_map.keys.compact
          owner_keys = if reflection.foreign_key.is_a?(Array)
            owners.map do |owner|
              Array(owner_key_name).map do |owner_key|
                owner[owner_key]
              end
            end.compact.uniq
          else
            owners_map.keys.compact
          end

          # Each record may have multiple owners, and vice-versa
          records_by_owner = owners.each_with_object({}) do |owner,h|
            h[owner] = []
          end

          if owner_keys.any?
            # Some databases impose a limit on the number of ids in a list (in Oracle it's 1000)
            # Make several smaller queries if necessary or make one query if the adapter supports it
            sliced  = owner_keys.each_slice(klass.connection.in_clause_length || owner_keys.size)

            records = load_slices sliced
            records.each do |record, owner_key|

              # Code requires Raven (Sentry to be setup) and handles the
              # transliterated keys.
              if !owners_map.has_key?(owner_key)
                # BEAC-3499 Squelch not in map errors
                #e = StandardError.new("#{owner_key} was not in map")
                #Raven.capture_exception(e)

                # Add transliterated key
                transliterate_keys(owners_map)
                # Mutate the owner_key to the transliterated version
                owner_key = I18n.transliterate(owner_key)
              end

              owners_map[owner_key].each do |owner|
                records_by_owner[owner] << record
              end
            end
          end

          records_by_owner
        end

        def load_slices(slices)
          @preloaded_records = slices.flat_map { |slice|
            records_for(slice)
          }

          # CPK
          # @preloaded_records.map { |record|
          #   key = record[association_key_name]
          #   key = key.to_s if key_conversion_required?
          #
          #   [record, key]
          # }
          @preloaded_records.map { |record|
            key = Array(association_key_name).map do |key_name|
              record[key_name].to_s.downcase.rstrip
            end.join(CompositePrimaryKeys::ID_SEP)

            [record, key]
          }
        end

        def owners_by_key
          @owners_by_key ||= if key_conversion_required?
                               owners.group_by do |owner|
                                 # CPK
                                 # owner[owner_key_name].to_s
                                 Array(owner_key_name).map do |key_name|
                                   owner[key_name].to_s.downcase.rstrip
                                 end.join(CompositePrimaryKeys::ID_SEP)
                               end
                             else
                               owners.group_by do |owner|
                                 # CPK
                                 # owner[owner_key_name]
                                 Array(owner_key_name).map do |key_name|
                                   owner[key_name].to_s.downcase.rstrip
                                 end.join(CompositePrimaryKeys::ID_SEP)
                               end
                             end
        end

        # Takes the owners_map and adds all the transliterated versions
        # of the keys to reference the same values. It will not allow transliterated
        # keys to collid with an original key or another transliterated key. It
        # mutates the owners map.
        def transliterate_keys(owners_map)
          transliterated_keys = {}
          duplicated_keys = Set.new
          owners_map.keys.each do |k|
            transliterated_key = I18n.transliterate(k)
            next if transliterated_key == k # No originals
            if transliterated_keys.include?(transliterated_key)
              duplicated_keys.add(transliterated_key)
            elsif !owners_map.include?(transliterated_key)
              transliterated_keys[transliterated_key] = owners_map[k]
            end
          end
          duplicated_keys.each do |k|
            transliterated_keys.delete(k)
          end
          owners_map.merge!(transliterated_keys)
          transliterated_keys
        end

      end
    end
  end
end
