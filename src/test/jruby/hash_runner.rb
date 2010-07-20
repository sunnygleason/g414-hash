
CLASSPATH=ENV["CLASSPATH"]

OPTS = [
  "-Xmx200m",
  "-DoutputDir=/tmp/faban",
  "-DdriverModule=com.g414.hash.impl.HashDriver\\$GuiceModule",
  "-DrampUp=10",
  "-DsteadyState=10",
  "-DrampDown=10",
].join(" ")


THREAD_PARAMS = [1]
HASH_PARAMS   = [
  "com.g414.hash.impl.JenkinsHash",
  "com.g414.hash.impl.Fnv1aHash",
  "com.g414.hash.impl.MurmurHash",
  "com.g414.hash.impl.CWowHash",
  "com.g414.hash.impl.Sha1PrngHash"
]

# THREAD_PARAMS = [4, 4, 4, 4, 16, 16, 16, 16]
# THREAD_PARAMS = [1, 1, 2, 4]
# THREAD_PARAMS = [1, 1, 2, 4, 6, 8, 12, 16, 24, 32, 64]

THREAD_PARAMS.each do |n|
HASH_PARAMS.each do |h|
  cmd = "java -cp \"#{CLASSPATH}\" #{OPTS} -Dthreads=#{n} -Dlonghash=#{h} com.sun.faban.driver.engine.GuiceMasterImpl"
  puts cmd
  `#{cmd}`
end
end

