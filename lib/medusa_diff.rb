require_relative 'find_diff'

class MedusaDiff 
    def self.run(repos)
        diff = FindDiff.new
        diff.run(repos)
    end
end