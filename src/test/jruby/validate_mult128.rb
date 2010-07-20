
require 'java'

import 'com.g414.hash.LongHashMethods'
import 'java.math.BigInteger'
import 'java.util.Random'

N = ARGV.shift.to_i

p0 = [0,0].to_java :long
p1 = [0,0].to_java :long

r = Random.new

i = 0

N.times do
  a = r.next_long
  b = r.next_long

  LongHashMethods::multiply128(a, b, p0)
  LongHashMethods::multiply128_optimized(a, b, p1)

  if (p0[0] != p1[0] && p0[1] != p1[1])
    puts "Successes: #{i}"
    puts "Failed on a=#{a}, b=#{b} : p0=#{p0[0]}:#{p0[1]}, p1=#{p1[0]}:#{p1[1]}"
    exit
  end

  i += 1
end

puts "Successes: #{i}"

